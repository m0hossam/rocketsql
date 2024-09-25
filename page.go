package main

import (
	"errors"
	"sync/atomic"
)

const (
	dbPageSize        uint16 = 4096
	dbHdrSize         uint16 = 6
	dbPageHdrSize     uint16 = 9
	dbNullPage        uint32 = 0
	dbMaxLeafCellSize uint16 = 4085
)

const (
	interiorPage uint8 = iota
	leafPage     uint8 = iota
)

const (
	offsetofPageType           = 0
	offsetofPageNumFreeBytes   = 1
	offsetofPageLastPtr        = 3
	offsetofPageNumCells       = 7
	offsetofPageCellOffsets    = 9
	offsetofPageOneDbPgSize    = 0
	offsetofPageOneDbFreePgPtr = 2
	sizeofPageType             = 1
	sizeofPageNumFreeBytes     = 2
	sizeofPageLastPtr          = 4
	sizeofPageNumCells         = 2
	sizeofPageCellOffset       = 2
	sizeofPageCellKey          = 4
	sizeofPageCellPtr          = 4
	sizeofPageCellPayloadSize  = 2
	sizeofPageOneDbPgSize      = 2
	sizeofPageOneDbFreePgPtr   = 4
	sizeofPageOneCellTableName = 32
)

type page struct {
	id          uint32
	pType       uint8
	nFreeBytes  uint16
	lastPtr     uint32
	nCells      uint16
	cellOffsets []uint16
	cells       []byte
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
		nFreeBytes:   dbPageSize - dbPageHdrSize - dbHdrSize,
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
		id:          *firstFreePtr,
		pType:       pType,
		nFreeBytes:  dbPageSize - dbPageHdrSize,
		lastPtr:     dbNullPage,
		nCells:      0,
		cellOffsets: []uint16{},
		cells:       []byte{},
	}
	atomic.AddUint32(firstFreePtr, 1)
	return p, nil
}
