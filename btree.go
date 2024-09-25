package main

import (
	"bytes"
	"encoding/binary"
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
			start := len(cur.cells) - (int(dbPageSize) - int(cur.cellOffsets[m]))
			k := binary.BigEndian.Uint32(cur.cells[start : start+sizeofPageCellKey])
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
			start := len(cur.cells) - (int(dbPageSize) - int(cur.cellOffsets[ind]))
			ptr = binary.BigEndian.Uint32(cur.cells[start+sizeofPageCellKey : start+sizeofPageCellKey+sizeofPageCellPtr])
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
		start := len(pg.cells) - (int(dbPageSize) - int(pg.cellOffsets[m]))
		k := binary.BigEndian.Uint32(pg.cells[start : start+sizeofPageCellKey])
		if k == key {
			payloadSize := int(binary.BigEndian.Uint16(pg.cells[start+sizeofPageCellKey : start+sizeofPageCellKey+sizeofPageCellPayloadSize]))
			return pg.cells[start+sizeofPageCellKey+sizeofPageCellPayloadSize : start+sizeofPageCellKey+sizeofPageCellPayloadSize+payloadSize], pg.id
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

func insertIntoLeaf(pg *page, key uint32, payload []byte) error {
	l := 0
	r := int(pg.nCells) - 1
	ind := int(pg.nCells) // index of 1st cell with a key > new key, is equal to the no. of cells if none found
	for l <= r {
		m := (l + r) / 2
		start := len(pg.cells) - (int(dbPageSize) - int(pg.cellOffsets[m]))
		k := binary.BigEndian.Uint32(pg.cells[start : start+sizeofPageCellKey])
		if k == key {
			return errors.New("the key is not unique")
		}
		if k < key {
			l = m + 1
		}
		if k > key {
			ind = m
			r = m - 1
		}
	}

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, key)
	binary.Write(buf, binary.BigEndian, uint16(len(payload))) // because payload size is stored in 2 bytes
	binary.Write(buf, binary.BigEndian, payload)
	binary.Write(buf, binary.BigEndian, pg.cells)

	cellSize := sizeofPageCellKey + sizeofPageCellPayloadSize + len(payload)
	cellOffset := dbPageHdrSize + pg.nCells*sizeofPageCellOffset + pg.nFreeBytes - uint16(cellSize)

	if ind == int(pg.nCells) { // append new cell offset at the end of the cell offsets array
		pg.cellOffsets = append(pg.cellOffsets, cellOffset)
	} else { // push cell offsets starting from [ind] forward one place & put new cell at [ind]
		pg.cellOffsets = append(pg.cellOffsets, 0)
		for i := len(pg.cellOffsets) - 1; i > ind; i-- { // use (len(cellOffsets)) instead of (pg.nCells) because we are mutating the latter
			pg.cellOffsets[i] = pg.cellOffsets[i-1]
		}
		pg.cellOffsets[ind] = cellOffset
	}

	pg.cells = buf.Bytes()
	pg.nFreeBytes -= sizeofPageCellOffset + uint16(cellSize)
	pg.nCells++

	return nil
}

func insert(key uint32, payload []byte, root *page, firstFreePtr *uint32) error {
	cellSize := sizeofPageCellKey + sizeofPageCellPayloadSize + len(payload)
	if cellSize > int(dbMaxLeafCellSize) {
		return errors.New("max leaf cell size exceeded: payload cannot fit in one page")
	}

	path := findPage(key, root)
	ptr := path[len(path)-1]
	pg, err := loadPage(ptr)
	if err != nil {
		return err
	}

	if uint16(cellSize) <= pg.nFreeBytes {
		err := insertIntoLeaf(pg, key, payload)
		if err == nil {
			err = savePage(pg)
		}
		return err
	}

	// TODO: Split case
	//maxNumCells := pg.nCells
	//minNumCells := (maxNumCells + 1) / 2
	// put minNumCells in current pg and remaining in newPg

	return errors.New("need to split node")
}
