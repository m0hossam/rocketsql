package main

import (
	"bytes"
	"encoding/binary"
)

func deserializePageOne(b []byte) *pageOne {
	p := &pageOne{}
	p.pSize = binary.BigEndian.Uint16(b[offsetofPageOneDbPgSize : offsetofPageOneDbPgSize+sizeofPageOneDbPgSize])
	p.firstFreePtr = binary.BigEndian.Uint32(b[offsetofPageOneDbFreePgPtr : offsetofPageOneDbFreePgPtr+sizeofPageOneDbFreePgPtr])
	p.pType = uint8(b[dbHdrSize+offsetofPageType])
	p.nFreeBytes = binary.BigEndian.Uint16(b[dbHdrSize+offsetofPageNumFreeBytes : dbHdrSize+offsetofPageNumFreeBytes+sizeofPageNumFreeBytes])
	p.lastPtr = binary.BigEndian.Uint32(b[dbHdrSize+offsetofPageLastPtr : dbHdrSize+offsetofPageLastPtr+sizeofPageLastPtr])
	p.nCells = binary.BigEndian.Uint16(b[dbHdrSize+offsetofPageNumCells : dbHdrSize+offsetofPageNumCells+sizeofPageNumCells])
	p.cellOffsets = []uint16{}
	for i := 0; i < int(p.nCells); i++ {
		start := int(dbHdrSize) + offsetofPageCellOffsets + i*sizeofPageCellOffset
		end := start + sizeofPageCellOffset
		p.cellOffsets = append(p.cellOffsets, binary.BigEndian.Uint16(b[start:end]))
	}
	p.cells = b[dbHdrSize+dbPageHdrSize+(p.nCells*sizeofPageCellOffset)+p.nFreeBytes : dbPageSize]
	return p
}

func deserializePage(ptr uint32, b []byte) *page {
	p := &page{}
	p.id = ptr
	p.pType = uint8(b[offsetofPageType])
	p.nFreeBytes = binary.BigEndian.Uint16(b[offsetofPageNumFreeBytes : offsetofPageNumFreeBytes+sizeofPageNumFreeBytes])
	p.lastPtr = binary.BigEndian.Uint32(b[offsetofPageLastPtr : offsetofPageLastPtr+sizeofPageLastPtr])
	p.nCells = binary.BigEndian.Uint16(b[offsetofPageNumCells : offsetofPageNumCells+sizeofPageNumCells])
	p.cellOffsets = []uint16{}
	for i := 0; i < int(p.nCells); i++ {
		start := offsetofPageCellOffsets + i*sizeofPageCellOffset
		end := start + sizeofPageCellOffset
		p.cellOffsets = append(p.cellOffsets, binary.BigEndian.Uint16(b[start:end]))
	}
	p.cells = b[dbPageHdrSize+(p.nCells*sizeofPageCellOffset)+p.nFreeBytes : dbPageSize]
	return p
}

func serializePage(p *page) []byte {
	buf := new(bytes.Buffer)
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
