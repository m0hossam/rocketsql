package main

import (
	"bytes"
	"encoding/binary"
)

func deserializePageOne(b []byte) *pageOne {
	p := &pageOne{}
	p.pSize = binary.BigEndian.Uint16(b[offsetofDbPgSize : offsetofDbPgSize+sizeofDbPgSize])
	p.firstFreePtr = binary.BigEndian.Uint32(b[offsetofDbFirstFreePgPtr : offsetofDbFirstFreePgPtr+sizeofDbFirstFreePgPtr])
	p.pType = uint8(b[dbHdrSize+offsetofPageType])
	p.nFreeBytes = binary.BigEndian.Uint16(b[dbHdrSize+offsetofNumFreeBytes : dbHdrSize+offsetofNumFreeBytes+sizeofNumFreeBytes])
	p.lastPtr = binary.BigEndian.Uint32(b[dbHdrSize+offsetofLastPtr : dbHdrSize+offsetofLastPtr+sizeofLastPtr])
	p.nCells = binary.BigEndian.Uint16(b[dbHdrSize+offsetofNumCells : dbHdrSize+offsetofNumCells+sizeofNumCells])
	p.cellOffsets = []uint16{}
	for i := 0; i < int(p.nCells); i++ {
		start := int(dbHdrSize) + 9 + i*sizeofCellOff
		end := start + sizeofCellOff
		p.cellOffsets = append(p.cellOffsets, binary.BigEndian.Uint16(b[start:end]))
	}
	p.cells = b[dbHdrSize+9+(p.nCells*sizeofCellOff)+p.nFreeBytes : dbPageSize]
	return p
}

func deserializePage(ptr uint32, b []byte) *page {
	p := &page{
		id:         ptr,
		pType:      uint8(b[offsetofPageType]),
		nFreeBytes: binary.BigEndian.Uint16(b[offsetofNumFreeBytes : offsetofNumFreeBytes+sizeofNumFreeBytes]),
		lastPtr:    binary.BigEndian.Uint32(b[offsetofLastPtr : offsetofLastPtr+sizeofLastPtr]),
		nCells:     binary.BigEndian.Uint16(b[offsetofNumCells : offsetofNumCells+sizeofNumCells]),
		cellOffArr: []uint16{},
		cells:      map[uint16]cell{},
	}

	freelistOff := binary.BigEndian.Uint16(b[offsetofFirstFreeBlkOff : offsetofFirstFreeBlkOff+sizeofFirstFreeBlkOff])
	var head *freeBlk
	var cur *freeBlk
	var prev *freeBlk
	for freelistOff != 0 {
		cur = &freeBlk{
			offset: freelistOff,
			size:   binary.BigEndian.Uint16(b[freelistOff+sizeofBlkNextOff : freelistOff+sizeofBlkNextOff+sizeofBlkSize]),
		}
		if head == nil {
			head = cur
			prev = cur
		} else {
			prev.nextBlk = cur
		}
		freelistOff = binary.BigEndian.Uint16(b[freelistOff : freelistOff+sizeofBlkNextOff])
	}
	p.freeBlkList = head

	for i := 0; i < int(p.nCells); i++ {
		offset := binary.BigEndian.Uint16(b[offsetofCellOffArr+i*sizeofCellOff : offsetofCellOffArr+i*sizeofCellOff+sizeofCellOff])
		c := cell{
			key: binary.BigEndian.Uint32(b[offset : offset+sizeofCellKey]),
		}
		if p.pType == leafPage {
			c.payloadSize = binary.BigEndian.Uint16(b[offset+sizeofCellKey : offset+sizeofCellKey+sizeofCellPayloadSize])
			c.payload = b[offset+sizeofCellKey+sizeofCellPayloadSize : offset+sizeofCellKey+sizeofCellPayloadSize+c.payloadSize]
		} else {
			c.ptr = binary.BigEndian.Uint32(b[offset+sizeofCellKey : offset+sizeofCellKey+sizeofCellPtr])
		}

		p.cellOffArr = append(p.cellOffArr, offset)
		p.cells[offset] = c
	}

	return p
}

func serializePage(p *page) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, p.pType)
	binary.Write(buf, binary.BigEndian, p.nFreeBytes)
	binary.Write(buf, binary.BigEndian, p.lastPtr)
	binary.Write(buf, binary.BigEndian, p.nCells)
	if p.freeBlkList != nil {
		binary.Write(buf, binary.BigEndian, p.freeBlkList.offset)
	} else {
		binary.Write(buf, binary.BigEndian, uint16(0))
	}
	for _, off := range p.cellOffArr {
		binary.Write(buf, binary.BigEndian, off)
	}
	binary.Write(buf, binary.BigEndian, bytes.Repeat([]byte{0}, int(dbPageSize-dbPageHdrSize-sizeofCellOff*p.nCells)))

	b := buf.Bytes()
	for off, cell := range p.cells { // serialize cells
		cellBuf := new(bytes.Buffer)
		binary.Write(cellBuf, binary.BigEndian, cell.key)
		var cellSize uint16
		if p.pType == leafPage {
			binary.Write(cellBuf, binary.BigEndian, cell.payloadSize)
			binary.Write(cellBuf, binary.BigEndian, cell.payload)
			cellSize = sizeofCellKey + sizeofCellPayloadSize + cell.payloadSize
		} else {
			binary.Write(cellBuf, binary.BigEndian, cell.ptr)
			cellSize = sizeofCellKey + sizeofCellPtr
		}
		copy(b[off:off+cellSize], cellBuf.Bytes())
	}

	head := p.freeBlkList
	for head != nil { // serialize free blocks
		blkBuf := new(bytes.Buffer)

		if head.nextBlk != nil {
			binary.Write(blkBuf, binary.BigEndian, head.nextBlk.offset)
		} else {
			binary.Write(blkBuf, binary.BigEndian, uint16(0))
		}
		binary.Write(blkBuf, binary.BigEndian, head.size)
		binary.Write(blkBuf, binary.BigEndian, bytes.Repeat([]byte{0}, int(head.size)-sizeofBlkNextOff-sizeofBlkSize))

		copy(b[head.offset:head.offset+head.size], blkBuf.Bytes())
		head = head.nextBlk
	}

	return b
}

func serializePageOne(p *pageOne) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, p.pSize)
	binary.Write(buf, binary.BigEndian, p.firstFreePtr)
	binary.Write(buf, binary.BigEndian, p.pType)
	binary.Write(buf, binary.BigEndian, p.nFreeBytes)
	binary.Write(buf, binary.BigEndian, p.lastPtr)
	binary.Write(buf, binary.BigEndian, p.nCells)
	for _, offset := range p.cellOffsets {
		binary.Write(buf, binary.BigEndian, offset)
	}
	binary.Write(buf, binary.BigEndian, bytes.Repeat([]byte{0}, int(p.nFreeBytes)))
	binary.Write(buf, binary.BigEndian, p.cells)
	return buf.Bytes()
}
