package main

import (
	"bytes"
	"encoding/binary"
)

func serializePage(p *page) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, p.pType)
	if p.freeList != nil {
		binary.Write(buf, binary.BigEndian, p.freeList.offset)
	} else {
		binary.Write(buf, binary.BigEndian, uint16(0))
	}
	binary.Write(buf, binary.BigEndian, p.nCells)
	binary.Write(buf, binary.BigEndian, p.cellArrOff)
	binary.Write(buf, binary.BigEndian, p.nFragBytes)
	binary.Write(buf, binary.BigEndian, p.lastPtr)
	for _, off := range p.cellPtrArr {
		binary.Write(buf, binary.BigEndian, off)
	}
	binary.Write(buf, binary.BigEndian, bytes.Repeat([]byte{0}, int(dbPageSize-dbPageHdrSize-sizeofCellOff*p.nCells)))

	b := buf.Bytes()

	for off, cell := range p.cells { // serialize cells
		cellBuf := new(bytes.Buffer)
		cellSize := 2 + len(cell.key) + len(cell.value)
		binary.Write(cellBuf, binary.BigEndian, uint16(len(cell.key)))
		binary.Write(cellBuf, binary.BigEndian, cell.key)
		if p.pType == leafPage { // for interior pages, cell.value is a uint32 page no.
			binary.Write(cellBuf, binary.BigEndian, uint16(len(cell.value)))
			cellSize += 2
		}
		binary.Write(cellBuf, binary.BigEndian, cell.value)
		copy(b[off:off+uint16(cellSize)], cellBuf.Bytes())
	}

	head := p.freeList
	for head != nil { // serialize free blocks
		blkBuf := new(bytes.Buffer)
		binary.Write(blkBuf, binary.BigEndian, head.size)
		if head.next != nil {
			binary.Write(blkBuf, binary.BigEndian, head.next.offset)
		} else {
			binary.Write(blkBuf, binary.BigEndian, uint16(0))
		}
		binary.Write(blkBuf, binary.BigEndian, bytes.Repeat([]byte{0}, int(head.size)-sizeofFreeBlockHdr))
		copy(b[head.offset:head.offset+head.size], blkBuf.Bytes())
		head = head.next
	}

	return b
}

func deserializePage(ptr uint32, b []byte) *page {
	p := &page{
		id:         ptr,
		pType:      uint8(b[offsetofPageType]),
		nCells:     binary.BigEndian.Uint16(b[offsetofNumCells : offsetofNumCells+sizeofNumCells]),
		cellArrOff: binary.BigEndian.Uint16(b[offsetofCellArrOff : offsetofCellArrOff+sizeofCellArrOff]),
		nFragBytes: uint8(b[offsetofNumFragBytes]),
		lastPtr:    binary.BigEndian.Uint32(b[offsetofLastPtr : offsetofLastPtr+sizeofLastPtr]),
		cellPtrArr: []uint16{},
		cells:      map[uint16]cell{},
	}

	freelistOff := binary.BigEndian.Uint16(b[offsetofFreeListOff : offsetofFreeListOff+sizeofFreeListOff])
	var head *freeBlock
	var prev *freeBlock
	for freelistOff != 0 {
		cur := &freeBlock{
			offset: freelistOff,
			size:   binary.BigEndian.Uint16(b[freelistOff+offsetofFreeBlockSize : freelistOff+offsetofFreeBlockSize+sizeofFreeBlockSize]),
		}
		if head == nil {
			head = cur
		} else {
			prev.next = cur
		}
		prev = cur
		freelistOff = binary.BigEndian.Uint16(b[freelistOff+offsetofFreeBlockNextOff : freelistOff+offsetofFreeBlockNextOff+sizeofFreeBlockNextOff])
	}
	p.freeList = head

	for i := 0; i < int(p.nCells); i++ {
		offset := binary.BigEndian.Uint16(b[offsetofCellPtrArr+i*sizeofCellOff : offsetofCellPtrArr+i*sizeofCellOff+sizeofCellOff])

		keySize := binary.BigEndian.Uint16(b[offset : offset+sizeofCellKeySize])
		c := cell{
			key: b[offset+sizeofCellKeySize : offset+sizeofCellKeySize+keySize],
		}
		if p.pType == leafPage {
			valueSize := binary.BigEndian.Uint16(b[offset+sizeofCellKeySize+keySize : offset+sizeofCellKeySize+keySize+sizeofCellValueSize])
			c.value = b[offset+sizeofCellKeySize+keySize+sizeofCellValueSize : offset+sizeofCellKeySize+keySize+sizeofCellValueSize+valueSize]
		} else {
			c.value = b[offset+sizeofCellKeySize+keySize : offset+sizeofCellKeySize+keySize+4] // uint32 page no.
		}

		p.cellPtrArr = append(p.cellPtrArr, offset)
		p.cells[offset] = c
	}

	return p
}
