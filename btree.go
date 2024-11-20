package main

import (
	"encoding/binary"
	"errors"
)

/*
Simplifying assumptions:
- Max cell size is (dbPageSize - dbPageHdrSize - 4*sizeofCellOffset) / 4
- to avoid cases where you need to create two new pages for insertion
*/

func getPath(key []byte, root *page) []uint32 { // returns page numbers from the root of the table the to leaf that should contain the key
	cur := root
	path := []uint32{root.id}
	for cur.pType != leafPage { // find the leaf page that should contain the key, takes O(H * log(N)) where H is the tree height and N is the max no. of keys in a page
		l := 0
		r := int(cur.nCells) - 1
		ind := int(cur.nCells)
		for l <= r { // binary search
			m := (l + r) / 2
			c := cur.cells[cur.cellPtrArr[m]]
			k := c.key
			cmp := compare(k, key)
			if cmp == firstEqualSecond {
				ind = m + 1
				break
			}
			if cmp == firstLessThanSecond {
				l = m + 1
			}
			if cmp == firstGreaterThanSecond {
				ind = m
				r = m - 1
			}
		}

		ptr := dbNullPage
		if ind == int(cur.nCells) {
			ptr = cur.lastPtr
		} else {
			c := cur.cells[cur.cellPtrArr[ind]]
			ptr = binary.BigEndian.Uint32(c.value)
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

func find(key []byte, root *page) ([]byte, uint32) { // returns raw tuple data and the containing page number
	path := getPath(key, root)
	ptr := path[len(path)-1]
	pg, err := loadPage(ptr)
	if err != nil {
		return nil, dbNullPage
	}

	l := 0
	r := int(pg.nCells) - 1
	for l <= r { // binary search
		m := (l + r) / 2
		c := pg.cells[pg.cellPtrArr[m]]
		k := c.key
		cmp := compare(k, key)
		if cmp == firstEqualSecond {
			return c.value, pg.id
		}
		if cmp == firstLessThanSecond {
			l = m + 1
		}
		if cmp == firstGreaterThanSecond {
			r = m - 1
		}
	}
	return nil, dbNullPage
}

func upperBoundIndex(pg *page, key []byte) (int, error) { // returns index of 1st cell with a key > new key, or the no. of cells if none found
	l := 0
	r := int(pg.nCells) - 1
	ind := int(pg.nCells)
	for l <= r {
		m := (l + r) / 2
		c := pg.cells[pg.cellPtrArr[m]]
		k := c.key
		cmp := compare(k, key)
		if cmp == firstEqualSecond {
			return 0, errors.New("the key is not unique")
		}
		if cmp == firstLessThanSecond {
			l = m + 1
		}
		if cmp == firstGreaterThanSecond {
			ind = m
			r = m - 1
		}
	}
	return ind, nil
}

func insertCell(pg *page, c cell, ind int, newOff uint16) {
	pg.cellPtrArr = append(pg.cellPtrArr, 0)
	for i := len(pg.cellPtrArr) - 1; i > ind; i-- { // shifting
		pg.cellPtrArr[i] = pg.cellPtrArr[i-1]
	}
	pg.cellPtrArr[ind] = newOff

	pg.cells[newOff] = c
	pg.nCells++

	if newOff < pg.cellArrOff {
		pg.cellArrOff = newOff
	}
}

func defragPage(pg *page) {
	cells := []cell{}
	ptr := pg.lastPtr
	for _, off := range pg.cellPtrArr {
		cells = append(cells, pg.cells[off])
	}
	truncatePage(pg)

	pg.lastPtr = ptr
	newOff := uint16(dbPageSize)
	for i, c := range cells {
		cellSize := uint16(2 + len(c.key) + len(c.value))
		if pg.pType == leafPage {
			cellSize += 2
		}
		newOff -= cellSize
		insertCell(pg, c, i, newOff) // should always succeed
	}
}

func insertIntoPage(pg *page, c cell, ind int) error {
	cellSize := uint16(2 + len(c.key) + len(c.value))
	if pg.pType == leafPage {
		cellSize += 2
	}

	var prev *freeBlock = nil
	head := pg.freeList
	freeBlocksSize := uint16(0)
	for head != nil { // first-fit singly linked list traversal
		freeBlocksSize += head.size

		if cellSize <= head.size {
			remSz := head.size - cellSize
			if remSz >= 4 { // new block takes upper half of old block
				head.size = remSz
				newOff := head.offset + remSz
				insertCell(pg, c, ind, newOff) // should always succeed
			} else { // new frag takes lower half
				if prev != nil {
					prev.next = head.next // remove the current block
				}
				newOff := head.offset
				insertCell(pg, c, ind, newOff) // should always succeed
				pg.nFragBytes += uint8(remSz)  // TODO: defrag if overflow happens
			}
			return nil
		}

		prev = head
		head = head.next
	}

	unallocatedSpace := pg.cellArrOff - offsetofCellPtrArr - sizeofCellOff // unallocated space minus sizeof cell offset (2 bytes)
	if unallocatedSpace >= cellSize {
		newOff := pg.cellArrOff - cellSize
		insertCell(pg, c, ind, newOff) // should always succeed
		return nil
	}

	if unallocatedSpace+uint16(pg.nFragBytes)+freeBlocksSize >= cellSize {
		defragPage(pg)
		return insertIntoPage(pg, c, ind) // should always succeed
	}

	return errors.New("page does not have enough space, need to split")
}

func getOverfullCellArr(pg *page, newCell cell, ind int) []cell {
	cells := []cell{}
	for i := 0; i < ind; i++ {
		c := pg.cells[pg.cellPtrArr[i]]
		cells = append(cells, c)
	}
	cells = append(cells, newCell)
	for i := ind; i < len(pg.cellPtrArr); i++ {
		c := pg.cells[pg.cellPtrArr[i]]
		cells = append(cells, c)
	}

	return cells
}

func insert(rootPg *page, firstFreePtr *uint32, key []byte, value []byte, leafInsert bool, path []uint32, oldChild uint32, newChild uint32) error {
	cellSize := len(key) + len(value) + 2
	if leafInsert {
		cellSize += 2
	}
	if cellSize > dbMaxCellSize {
		return errors.New("max cell size exceeded")
	}

	newCell := cell{
		key:   key,
		value: value,
	}

	if leafInsert {
		path = getPath(key, rootPg)
	}

	if len(path) == 0 { // creating new root
		newPg, err := createPage(rootPg.pType, firstFreePtr)
		if err != nil {
			return err
		}

		copyPage(newPg, rootPg)

		truncatePage(rootPg)
		rootPg.pType = interiorPage
		rootPg.lastPtr = newChild
		insertCell(rootPg, newCell, 0, uint16(dbPageSize-len(newCell.key)-len(newCell.value)-4))

		err = savePage(rootPg)
		if err != nil {
			return err
		}
		return saveNewPage(newPg)
	}

	pg, err := loadPage(path[len(path)-1])
	if err != nil {
		return err
	}

	ind, err := upperBoundIndex(pg, key)
	if err != nil {
		return err
	}

	if !leafInsert {
		if ind < int(pg.nCells) {
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, newChild)
			c := pg.cells[pg.cellPtrArr[ind]]
			c.value = buf
			pg.cells[pg.cellPtrArr[ind]] = c
		} else {
			pg.lastPtr = newChild
		}
	}

	err = insertIntoPage(pg, newCell, ind)
	if err == nil {
		return savePage(pg)
	}

	// Split case

	cells := getOverfullCellArr(pg, newCell, ind)
	tempPtr := pg.lastPtr
	mid := len(cells) / 2
	if leafInsert {
		mid = (len(cells) + 1) / 2
	}

	truncatePage(pg)
	for i := 0; i < mid; i++ {
		err = insertIntoPage(pg, cells[i], i)
		if err != nil {
			return err
		}
	}

	newPg, err := createPage(interiorPage, firstFreePtr)
	if err != nil {
		return err
	}
	if leafInsert {
		newPg.pType = leafPage
	}
	newPg.lastPtr = tempPtr

	for i := mid + 1; i < len(cells); i++ {
		err = insertIntoPage(newPg, cells[i], i-mid-1)
		if err != nil {
			return err
		}
	}

	if leafInsert {
		pg.lastPtr = newPg.id
		insertIntoPage(pg, cells[mid], mid)
		mid = mid + 1 // first cell in new page
	} else {
		pg.lastPtr = binary.BigEndian.Uint32(cells[mid].value)
	}

	err = insert(rootPg, firstFreePtr, cells[mid].key, cells[mid].value, false, path[:len(path)-1], pg.id, newPg.id)
	if err != nil {
		return err
	}

	err = savePage(rootPg)
	if err != nil {
		return err
	}
	err = savePage(pg)
	if err != nil {
		return err
	}
	err = saveNewPage(newPg)
	if err != nil {
		return err
	}

	return nil
}
