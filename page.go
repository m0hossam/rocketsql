package main

import (
	"errors"
	"sync/atomic"
)

const (
	dbPageSize                = 512 // default is 4096 but can be any power of two between 512 and 65536
	dbPageHdrSize             = 12
	dbMaxLeafCellSize         = dbPageSize - dbPageHdrSize - sizeofCellOff
	dbMinFreeBlockSize        = 3
	dbNullPage         uint32 = 0
)

const (
	interiorPage uint8 = iota
	leafPage     uint8 = iota
)

const (
	offsetofPageType      = 0
	offsetofFreeListOff   = 1
	offsetofNumCells      = 3
	offsetofCellArrOff    = 5
	offsetofNumFragBytes  = 7
	offsetofLastPtr       = 8
	offsetofCellPtrArrOff = 12
	sizeofPageType        = 1
	sizeofFreeListOff     = 2
	sizeofNumCells        = 2
	sizeofCellArrOff      = 2
	sizeofNumFragBytes    = 1
	sizeofLastPtr         = 4
	sizeofCellOff         = 2
)

const (
	offsetofDbPageSize    = 0
	offsetofDbFreePagePtr = 2
	sizeofDbPageSize      = 2
	sizeofDbFreePagePtr   = 4
)

const (
	offsetofFreeBlockNextOff = 0
	offsetofFreeBlockSize    = 2
	sizeofFreeBlockNextOff   = 2
	sizeofFreeBlockSize      = 2
)

const (
	sizeofCellKeySize   = 2
	sizeofCellValueSize = 2
)

type freeBlock struct {
	offset uint16
	size   uint16
	next   *freeBlock
}

type cell struct {
	key   []byte
	value []byte
}

type page struct {
	id         uint32
	pType      uint8
	freeList   *freeBlock
	nCells     uint16
	cellArrOff uint16
	nFragBytes uint8
	lastPtr    uint32
	cellPtrArr []uint16
	cells      map[uint16]cell
}

func createPage(pType uint8, firstFreePtr *uint32) (*page, error) {
	if pType != interiorPage && pType != leafPage { // invalid type
		return nil, errors.New("invalid page type")
	}
	p := &page{
		id:         *firstFreePtr,
		pType:      pType,
		nCells:     0,
		cellArrOff: dbPageSize,
		nFragBytes: 0,
		lastPtr:    dbNullPage,
		cellPtrArr: []uint16{},
		cells:      map[uint16]cell{},
	}
	atomic.AddUint32(firstFreePtr, 1)
	return p, nil
}

func truncatePage(p *page) {
	p.nCells = 0
	p.cellArrOff = dbPageSize
	p.nFragBytes = 0
	p.lastPtr = dbNullPage
	p.cellPtrArr = []uint16{}
	p.cells = map[uint16]cell{}
}
