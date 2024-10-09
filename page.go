package main

import (
	"errors"
	"sync/atomic"
)

const (
	dbPageSize        uint16 = 64
	dbHdrSize         uint16 = 6
	dbPageHdrSize     uint16 = 11
	dbNullPage        uint32 = 0
	dbMaxLeafCellSize uint16 = dbPageSize - dbPageHdrSize - sizeofCellOff
)

const (
	interiorPage uint8 = iota
	leafPage     uint8 = iota
)

const (
	offsetofPageType         = 0
	offsetofNumFreeBytes     = 1
	offsetofLastPtr          = 3
	offsetofNumCells         = 7
	offsetofFirstFreeBlkOff  = 9
	offsetofCellOffArr       = 11
	offsetofDbPgSize         = 0
	offsetofDbFirstFreePgPtr = 2
	sizeofPageType           = 1
	sizeofNumFreeBytes       = 2
	sizeofLastPtr            = 4
	sizeofNumCells           = 2
	sizeofFirstFreeBlkOff    = 2
	sizeofCellOff            = 2
	sizeofCellKey            = 4
	sizeofCellPtr            = 4
	sizeofCellPayloadSize    = 2
	sizeofCellTableName      = 32
	sizeofBlkNextOff         = 2
	sizeofBlkSize            = 2
	sizeofDbPgSize           = 2
	sizeofDbFirstFreePgPtr   = 4
)

type freeBlk struct {
	offset  uint16
	size    uint16
	nextBlk *freeBlk
}

type cell struct {
	key         uint32
	ptr         uint32
	payloadSize uint16
	payload     []byte
}

type page struct {
	id          uint32
	pType       uint8
	nFreeBytes  uint16
	lastPtr     uint32
	nCells      uint16
	freeBlkList *freeBlk
	cellOffArr  []uint16
	cells       map[uint16]cell
}

type pageOne struct {
	pSize        uint16
	firstFreePtr uint32
	pType        uint8
	nFreeBytes   uint16
	lastPtr      uint32
	nCells       uint16
	cellOffsets  []uint16
	cells        []byte
}

func createPageOne() *pageOne {
	p := &pageOne{
		pSize:        dbPageSize,
		firstFreePtr: 2,
		pType:        leafPage,
		nFreeBytes:   dbPageSize - 9 - dbHdrSize,
		lastPtr:      dbNullPage,
		nCells:       0,
		cellOffsets:  []uint16{},
		cells:        []byte{},
	}
	return p
}

func createPage(pType uint8, firstFreePtr *uint32) (*page, error) {
	if pType != interiorPage && pType != leafPage { // invalid type
		return nil, errors.New("invalid page type")
	}
	p := &page{
		id:         *firstFreePtr,
		pType:      pType,
		nFreeBytes: dbPageSize - dbPageHdrSize,
		lastPtr:    dbNullPage,
		nCells:     0,
		cellOffArr: []uint16{},
		cells:      map[uint16]cell{},
	}
	atomic.AddUint32(firstFreePtr, 1)
	return p, nil
}

func truncatePage(p *page) {
	p.cellOffArr = []uint16{}
	p.cells = map[uint16]cell{}
	p.nCells = 0
	p.nFreeBytes = dbPageSize - dbPageHdrSize
	p.freeBlkList = nil
}
