package main

import (
	"encoding/binary"
	"errors"
)

/*
Simplifying assumptions:
- Max cell size is designed such that at least 2 cells can fit in a page
- to avoid cases where we need to create two new pages for insertion
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

	if newOff < pg.cellArrOff { // keep track of closest cell to the top of the page
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
	if pg.nCells == dbMaxCellsPerPage {
		return errors.New("page does not have enough space (soft limit), need to split")
	}

	cellSize := uint16(2 + len(c.key) + len(c.value))
	if pg.pType == leafPage {
		cellSize += 2
	}

	unallocatedSpace := pg.cellArrOff - (offsetofCellPtrArr + sizeofCellOff*pg.nCells) // space between last cell ptr and first cell
	totalFreeSize := uint16(unallocatedSpace) + uint16(pg.nFragBytes)
	for head := pg.freeList; head != nil; head = head.next {
		totalFreeSize += head.size
	}

	if totalFreeSize < cellSize+sizeofCellOff {
		return errors.New("page does not have enough space, need to split")
	}

	if unallocatedSpace < sizeofCellOff {
		defragPage(pg)
		unallocatedSpace = pg.cellArrOff - (offsetofCellPtrArr + sizeofCellOff*pg.nCells) // variable must be recalculated here because it's used again
	}

	var prev *freeBlock = nil
	head := pg.freeList
	for head != nil { // first-fit singly linked list traversal
		if cellSize <= head.size {
			remSz := head.size - cellSize
			if remSz >= dbMinFreeBlockSize { // new block takes upper half of old block
				head.size = remSz
				newOff := head.offset + remSz
				insertCell(pg, c, ind, newOff) // should always succeed
			} else { // new frag takes lower half
				if prev != nil {
					prev.next = head.next // remove the current block
				} else { // remove head
					pg.freeList = nil
				}
				newOff := head.offset
				insertCell(pg, c, ind, newOff)           // should always succeed
				if int(pg.nFragBytes)+int(remSz) > 255 { // uint8 overflow precaution
					defragPage(pg)
				} else {
					pg.nFragBytes += uint8(remSz)
				}
			}
			return nil
		}

		prev = head
		head = head.next
	}

	if unallocatedSpace < cellSize+sizeofCellOff {
		defragPage(pg)
	}

	newOff := pg.cellArrOff - cellSize
	insertCell(pg, c, ind, newOff) // should always succeed

	return nil
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

func interiorInsert(path []uint32, key []byte, value []byte, newChild uint32, firstFreePtr *uint32) error {
	if (len(key) + len(value) + 2) > dbMaxCellSize {
		return errors.New("max cell size exceeded")
	}

	if len(path) == 0 { // creating new root
		rootPgNo := binary.BigEndian.Uint32(value)
		rootPg, err := loadPage(rootPgNo)
		if err != nil {
			return err
		}

		newPg, err := createPage(rootPg.pType, firstFreePtr)
		if err != nil {
			return err
		}

		valBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(valBuf, newPg.id)
		newCell := cell{
			key:   key,
			value: valBuf, // because value will point to the root
		}

		copyPage(newPg, rootPg)
		truncatePage(rootPg)
		rootPg.pType = interiorPage
		rootPg.lastPtr = newChild
		insertCell(rootPg, newCell, 0, uint16(dbPageSize-len(newCell.key)-len(newCell.value)-2))

		err = saveNewPage(newPg)
		if err != nil {
			return err
		}
		err = savePage(rootPg)
		if err != nil {
			return err
		}

		return nil
	}

	newCell := cell{
		key:   key,
		value: value,
	}
	pg, err := loadPage(path[len(path)-1])
	if err != nil {
		return err
	}
	ind, err := upperBoundIndex(pg, key)
	if err != nil {
		return err
	}
	if ind == int(pg.nCells) {
		pg.lastPtr = newChild
	} else {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, newChild)
		c := pg.cells[pg.cellPtrArr[ind]]
		c.value = buf
		pg.cells[pg.cellPtrArr[ind]] = c
	}
	err = insertIntoPage(pg, newCell, ind)
	if err == nil {
		return savePage(pg)
	}

	cells := getOverfullCellArr(pg, newCell, ind)
	mid := len(cells) / 2

	newPg, err := createPage(interiorPage, firstFreePtr)
	if err != nil {
		return err
	}
	newPg.lastPtr = newChild

	truncatePage(pg)
	pg.lastPtr = binary.BigEndian.Uint32(cells[mid].value)

	for i := 0; i < mid; i++ {
		err = insertIntoPage(pg, cells[i], i)
		if err != nil {
			return err
		}
	}
	for i := mid + 1; i < len(cells); i++ {
		err = insertIntoPage(newPg, cells[i], i-mid-1)
		if err != nil {
			return err
		}
	}

	err = savePage(pg)
	if err != nil {
		return err
	}
	err = saveNewPage(newPg)
	if err != nil {
		return err
	}

	valBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(valBuf, pg.id)
	err = interiorInsert(path[:len(path)-1], cells[mid].key, valBuf, newPg.id, firstFreePtr)
	if err != nil {
		return err
	}

	return nil
}

func insert(rootPg *page, key []byte, value []byte, firstFreePtr *uint32) error {
	if (len(key) + len(value) + 4) > dbMaxCellSize {
		return errors.New("max cell size exceeded")
	}

	newCell := cell{
		key:   key,
		value: value,
	}
	path := getPath(key, rootPg)
	pg, err := loadPage(path[len(path)-1])
	if err != nil {
		return err
	}
	ind, err := upperBoundIndex(pg, key)
	if err != nil {
		return err
	}
	err = insertIntoPage(pg, newCell, ind)
	if err == nil {
		return savePage(pg)
	}

	cells := getOverfullCellArr(pg, newCell, ind)
	mid := (len(cells) + 1) / 2

	newPg, err := createPage(leafPage, firstFreePtr)
	if err != nil {
		return err
	}
	newPg.lastPtr = pg.lastPtr

	truncatePage(pg)
	pg.lastPtr = newPg.id

	for i := 0; i < mid; i++ {
		err = insertIntoPage(pg, cells[i], i)
		if err != nil {
			return err
		}
	}
	for i := mid; i < len(cells); i++ {
		err = insertIntoPage(newPg, cells[i], i-mid)
		if err != nil {
			return err
		}
	}

	err = savePage(pg)
	if err != nil {
		return err
	}
	err = saveNewPage(newPg)
	if err != nil {
		return err
	}

	valBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(valBuf, pg.id)
	err = interiorInsert(path[:len(path)-1], newPg.cells[newPg.cellPtrArr[0]].key, valBuf, newPg.id, firstFreePtr)
	if err != nil {
		return err
	}

	return nil
}

func insertFreeBlock(pg *page, newFreeBlock *freeBlock) {
	if pg.freeList == nil || newFreeBlock.offset < pg.freeList.offset {
		newFreeBlock.next = pg.freeList
		pg.freeList = newFreeBlock
	} else {
		cur := pg.freeList
		for cur.next != nil && cur.next.offset < newFreeBlock.offset {
			cur = cur.next
		}
		newFreeBlock.next = cur.next
		cur.next = newFreeBlock
	}

	cur := pg.freeList
	for cur != nil && cur.next != nil {
		if cur.offset+cur.size == cur.next.offset { // merge contiguous blocks
			cur.size += cur.next.size
			cur.next = cur.next.next
		} else {
			cur = cur.next
		}
	}
}

func removeCell(pg *page, ind int) {
	off := pg.cellPtrArr[ind]
	c := pg.cells[off]
	cellSize := uint16(2 + len(c.key) + len(c.value))
	if pg.pType == leafPage {
		cellSize += 2
	}

	delete(pg.cells, off)
	pg.cellPtrArr = append(pg.cellPtrArr[:ind], pg.cellPtrArr[ind+1:]...)
	pg.nCells--

	if off == pg.cellArrOff {
		pg.cellArrOff += cellSize
	} else {
		newFreeBlock := &freeBlock{
			offset: off,
			size:   cellSize,
		}
		insertFreeBlock(pg, newFreeBlock)
	}
}

func remove(rootPg *page, key []byte, firstFreePtr *uint32) error {
	path := getPath(key, rootPg)
	ptr := path[len(path)-1]
	pg, err := loadPage(ptr)
	if err != nil {
		return err
	}

	l := 0
	r := int(pg.nCells) - 1
	ind := -1
	for l <= r { // binary search
		m := (l + r) / 2
		c := pg.cells[pg.cellPtrArr[m]]
		k := c.key
		cmp := compare(k, key)
		if cmp == firstEqualSecond {
			ind = m
			break
		}
		if cmp == firstLessThanSecond {
			l = m + 1
		}
		if cmp == firstGreaterThanSecond {
			r = m - 1
		}
	}

	if ind == -1 {
		return errors.New("key not found")
	}

	removeCell(pg, ind)
	err = savePage(pg)
	if err != nil {
		return err
	}

	return nil
}
