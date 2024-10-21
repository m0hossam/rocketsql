package main

import (
	"errors"
)

func findPage(key uint32, root *page) []uint32 { // returns page numbers from the root of the table the to leaf that should contain the key
	cur := root
	path := []uint32{root.id}
	for cur.pType != leafPage { // find the leaf page that should contain the key, takes O(H * log(N)) where H is the tree height and N is the max no. of keys in a page
		l := 0
		r := int(cur.nCells) - 1
		ind := int(cur.nCells)
		for l <= r { // binary search
			m := (l + r) / 2
			c := cur.cells[cur.cellOffArr[m]]
			k := c.key
			if k == key {
				ind = m + 1
				break
			}
			if k < key {
				l = m + 1
			}
			if k > key {
				ind = m
				r = m - 1
			}
		}

		ptr := dbNullPage
		if ind == int(cur.nCells) {
			ptr = cur.lastPtr
		} else {
			c := cur.cells[cur.cellOffArr[ind]]
			ptr = c.ptr
		}

		var err error
		cur, err = loadPage(ptr)
		if err != nil {
			return nil
		}
		path = append(path, cur.id)
	}
	return path
}

func find(key uint32, root *page) ([]byte, uint32) { // returns raw tuple data and the containing page number
	path := findPage(key, root)
	ptr := path[len(path)-1]
	pg, err := loadPage(ptr)
	if err != nil {
		return nil, dbNullPage
	}

	l := 0
	r := int(pg.nCells) - 1
	for l <= r { // binary search
		m := (l + r) / 2
		c := pg.cells[pg.cellOffArr[m]]
		k := c.key
		if k == key {
			return c.payload, pg.id
		}
		if k < key {
			l = m + 1
		}
		if k > key {
			r = m - 1
		}
	}
	return nil, dbNullPage
}

func upperBoundIndex(pg *page, key uint32) (int, error) { // returns index of 1st cell with a key > new key, or the no. of cells if none found
	l := 0
	r := int(pg.nCells) - 1
	ind := int(pg.nCells)
	for l <= r {
		m := (l + r) / 2
		c := pg.cells[pg.cellOffArr[m]]
		k := c.key
		if k == key {
			return 0, errors.New("the key is not unique")
		}
		if k < key {
			l = m + 1
		}
		if k > key {
			ind = m
			r = m - 1
		}
	}
	return ind, nil
}

func pushBackCell(pg *page, c cell) {
	cellSize := sizeofCellKey
	if pg.pType == interiorPage {
		cellSize += sizeofCellPtr
	} else {
		cellSize += sizeofCellPayloadSize + int(c.payloadSize)
	}

	off := dbPageSize - uint16(cellSize)*(pg.nCells+1)
	pg.cellOffArr = append(pg.cellOffArr, off)
	pg.cells[off] = c
	pg.nCells++
	pg.nFreeBytes -= uint16(cellSize + sizeofCellOff)
}

func insertIntoLeaf(pg *page, key uint32, payload []byte) error {
	c := cell{
		key:         key,
		payloadSize: uint16(len(payload)),
		payload:     payload,
	}

	var cellOffset uint16
	cellSize := sizeofCellKey + sizeofCellPayloadSize + c.payloadSize
	if pg.freeBlkList != nil {
		cellOffset = pg.freeBlkList.offset
		pg.freeBlkList = pg.freeBlkList.nextBlk
	} else {
		cellOffset = dbPageHdrSize + pg.nCells*sizeofCellOff + pg.nFreeBytes - cellSize
	}
	pg.cells[cellOffset] = c

	ind, err := upperBoundIndex(pg, key)
	if err != nil {
		return err
	}

	pg.cellOffArr = append(pg.cellOffArr, 0)
	for i := len(pg.cellOffArr) - 1; i > ind; i-- { // use (len(cellOffsets)) instead of (pg.nCells) because we are mutating the latter
		pg.cellOffArr[i] = pg.cellOffArr[i-1]
	}
	pg.cellOffArr[ind] = cellOffset

	pg.nFreeBytes -= sizeofCellOff + cellSize
	pg.nCells++

	return savePage(pg)
}

func insertIntoNonLeaf(firstFreePtr *uint32, path []uint32, key uint32, oldLeaf uint32, newLeaf uint32) (uint32, error) { // TODO: will need firstFreePtr

	newCell := cell{
		key: key,
		ptr: oldLeaf,
	}
	cellSize := sizeofCellKey + sizeofCellPtr

	if len(path) == 0 { // creating new root
		newRoot, err := createPage(interiorPage, firstFreePtr)
		if err != nil {
			return dbNullPage, err
		}
		newRoot.lastPtr = newLeaf
		off := dbPageSize - uint16(cellSize)
		newRoot.cellOffArr = append(newRoot.cellOffArr, off)
		newRoot.cells[off] = newCell
		newRoot.nCells++
		newRoot.nFreeBytes -= uint16(cellSize + sizeofCellOff)
		err = saveNewPage(newRoot)
		if err != nil {
			return dbNullPage, err
		}
		return newRoot.id, nil
	}

	pg, err := loadPage(path[len(path)-1])
	if err != nil {
		return dbNullPage, err
	}
	ind, err := upperBoundIndex(pg, key)
	if err != nil {
		return dbNullPage, err // duplicate key
	}

	if cellSize+sizeofCellOff > int(pg.nFreeBytes) { // TODO: Split case

		if ind == int(pg.nCells) {
			pg.lastPtr = newLeaf
		} else {
			c := pg.cells[pg.cellOffArr[ind]]
			c.ptr = newLeaf
			pg.cells[pg.cellOffArr[ind]] = c
		}

		cells := []cell{}
		for i := 0; i < ind; i++ {
			c := pg.cells[pg.cellOffArr[i]]
			cells = append(cells, c)
		}
		cells = append(cells, newCell)
		for i := ind; i < len(pg.cellOffArr); i++ {
			c := pg.cells[pg.cellOffArr[i]]
			cells = append(cells, c)
		}
		truncatePage(pg)

		newPg, err := createPage(interiorPage, firstFreePtr)
		if err != nil {
			return dbNullPage, err
		}

		idx := len(cells) / 2
		newPg.lastPtr = pg.lastPtr
		pg.lastPtr = cells[idx].ptr
		for i := 0; i < idx; i++ {
			pushBackCell(pg, cells[i])
		}
		for i := idx + 1; i < len(cells); i++ {
			pushBackCell(newPg, cells[i])
		}

		err = savePage(pg)
		if err != nil {
			return dbNullPage, err
		}

		err = saveNewPage(newPg)
		if err != nil {
			return dbNullPage, err
		}

		path = path[:len(path)-1] // remove last ptr in path
		return insertIntoNonLeaf(firstFreePtr, path, cells[idx].key, pg.id, newPg.id)
	}

	// no split needed:
	var off uint16
	if pg.freeBlkList != nil { // TODO: Fragmentation
		off = pg.freeBlkList.offset
		pg.freeBlkList = pg.freeBlkList.nextBlk
	} else {
		off = dbPageSize - uint16(cellSize)*(pg.nCells+1)
	}

	pg.cellOffArr = append(pg.cellOffArr, 0)
	if ind == int(pg.nCells) {
		pg.lastPtr = newLeaf
	} else { // shifting
		c := pg.cells[pg.cellOffArr[ind]]
		c.ptr = newLeaf
		pg.cells[pg.cellOffArr[ind]] = c
		for i := len(pg.cellOffArr) - 1; i > ind; i-- {
			pg.cellOffArr[i] = pg.cellOffArr[i-1]
		}
	}

	pg.cellOffArr[ind] = off
	pg.cells[off] = newCell
	pg.nCells++
	pg.nFreeBytes -= uint16(cellSize) + sizeofCellOff

	return dbNullPage, savePage(pg)
}

func insert(key uint32, payload []byte, root *page, firstFreePtr *uint32) (uint32, error) {
	cellSize := sizeofCellKey + sizeofCellPayloadSize + len(payload)
	if cellSize > int(dbMaxLeafCellSize) {
		return dbNullPage, errors.New("max leaf cell size exceeded: payload cannot fit in one page")
	}

	path := findPage(key, root)
	ptr := path[len(path)-1]
	pg, err := loadPage(ptr)
	if err != nil {
		return dbNullPage, err
	}

	if cellSize+sizeofCellOff <= int(pg.nFreeBytes) {
		return dbNullPage, insertIntoLeaf(pg, key, payload) // TODO: need to take fragmentation into account
	}

	// Split case:
	ind, err := upperBoundIndex(pg, key)
	if err != nil {
		return dbNullPage, err
	}

	cells := []cell{}
	for i := 0; i < ind; i++ {
		c := pg.cells[pg.cellOffArr[i]]
		cells = append(cells, c)
	}
	newCell := cell{
		key:         key,
		payloadSize: uint16(len(payload)),
		payload:     payload,
	}
	cells = append(cells, newCell)
	for i := ind; i < len(pg.cellOffArr); i++ {
		c := pg.cells[pg.cellOffArr[i]]
		cells = append(cells, c)
	}

	truncatePage(pg)
	for i := 0; i < (len(cells)+1)/2; i++ {
		off := dbPageSize - uint16(cellSize*(i+1)) // cells in same page belong to same table (i.e. fixed-length cells)
		pg.cellOffArr = append(pg.cellOffArr, off)
		pg.cells[off] = cells[i]
		pg.nCells++
		pg.nFreeBytes -= uint16(cellSize + sizeofCellOff)
	}

	newPg, err := createPage(leafPage, firstFreePtr)
	if err != nil {
		return dbNullPage, err
	}
	for i := (len(cells) + 1) / 2; i < len(cells); i++ {
		off := dbPageSize - uint16(cellSize*(i-(len(cells)+1)/2+1))
		newPg.cellOffArr = append(newPg.cellOffArr, off)
		newPg.cells[off] = cells[i]
		newPg.nCells++
		newPg.nFreeBytes -= uint16(cellSize + sizeofCellOff)
	}

	newPg.lastPtr = pg.lastPtr
	pg.lastPtr = newPg.id

	err = savePage(pg)
	if err != nil {
		return dbNullPage, err
	}

	err = saveNewPage(newPg)
	if err != nil {
		return dbNullPage, err
	}

	path = path[:len(path)-1] // remove last ptr in path
	newRootId, err := insertIntoNonLeaf(firstFreePtr, path, newPg.cells[newPg.cellOffArr[0]].key, pg.id, newPg.id)
	if err != nil {
		return dbNullPage, err
	}
	return newRootId, err // TODO: Since we save multiple pages sequentially, changes must be undone if somes pages saved and some failed
}
