package page

import (
	"bytes"
	"encoding/binary"
)

func (pg *Page) SerializePage() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, pg.Type)
	if pg.FreeList != nil {
		binary.Write(buf, binary.BigEndian, pg.FreeList.Offset)
	} else {
		binary.Write(buf, binary.BigEndian, uint16(0))
	}
	binary.Write(buf, binary.BigEndian, pg.NumCells)
	binary.Write(buf, binary.BigEndian, pg.CellArrOff)
	binary.Write(buf, binary.BigEndian, pg.NumFragBytes)
	binary.Write(buf, binary.BigEndian, pg.LastPtr)
	for _, off := range pg.CellPtrArr {
		binary.Write(buf, binary.BigEndian, off)
	}
	binary.Write(buf, binary.BigEndian, bytes.Repeat([]byte{0}, int(DefaultPageSize-HeaderSize-SizeOfCellOff*pg.NumCells)))

	b := buf.Bytes()

	for off, cell := range pg.Cells { // serialize Cells
		cellBuf := new(bytes.Buffer)
		cellSize := 2 + len(cell.Key) + len(cell.Value)
		binary.Write(cellBuf, binary.BigEndian, uint16(len(cell.Key)))
		binary.Write(cellBuf, binary.BigEndian, cell.Key)
		if pg.Type == LeafPage { // for interior pages, cell.Value is a uint32 page no.
			binary.Write(cellBuf, binary.BigEndian, uint16(len(cell.Value)))
			cellSize += 2
		}
		binary.Write(cellBuf, binary.BigEndian, cell.Value)
		copy(b[off:off+uint16(cellSize)], cellBuf.Bytes())
	}

	head := pg.FreeList
	for head != nil { // serialize free blocks
		blkBuf := new(bytes.Buffer)
		binary.Write(blkBuf, binary.BigEndian, head.Size)
		if head.Next != nil {
			binary.Write(blkBuf, binary.BigEndian, head.Next.Offset)
		} else {
			binary.Write(blkBuf, binary.BigEndian, uint16(0))
		}
		binary.Write(blkBuf, binary.BigEndian, bytes.Repeat([]byte{0}, int(head.Size)-SizeOfFreeBlockHdr))
		copy(b[head.Offset:head.Offset+head.Size], blkBuf.Bytes())
		head = head.Next
	}

	return b
}

func DeserializePage(ptr uint32, b []byte) *Page {
	p := &Page{
		Id:           ptr,
		Type:         uint8(b[OffsetOfPageType]),
		NumCells:     binary.BigEndian.Uint16(b[OffsetOfNumCells : OffsetOfNumCells+SizeOfNumCells]),
		CellArrOff:   binary.BigEndian.Uint16(b[OffsetOfCellArrOff : OffsetOfCellArrOff+SizeOfCellArrOff]),
		NumFragBytes: uint8(b[OffsetOfNumFragBytes]),
		LastPtr:      binary.BigEndian.Uint32(b[OffsetOfLastPtr : OffsetOfLastPtr+SizeOfLastPtr]),
		CellPtrArr:   []uint16{},
		Cells:        map[uint16]Cell{},
	}

	freelistOff := binary.BigEndian.Uint16(b[OffsetOfFreeListOff : OffsetOfFreeListOff+SizeOfFreeListOff])
	var head *FreeBlock
	var prev *FreeBlock
	for freelistOff != 0 {
		cur := &FreeBlock{
			Offset: freelistOff,
			Size:   binary.BigEndian.Uint16(b[freelistOff+OffsetOfFreeBlockSize : freelistOff+OffsetOfFreeBlockSize+SizeOfFreeBlockSize]),
		}
		if head == nil {
			head = cur
		} else {
			prev.Next = cur
		}
		prev = cur
		freelistOff = binary.BigEndian.Uint16(b[freelistOff+OffsetOfFreeBlockNextOff : freelistOff+OffsetOfFreeBlockNextOff+SizeOfFreeBlockNextOff])
	}
	p.FreeList = head

	for i := 0; i < int(p.NumCells); i++ {
		Offset := binary.BigEndian.Uint16(b[OffsetOfCellPtrArr+i*SizeOfCellOff : OffsetOfCellPtrArr+i*SizeOfCellOff+SizeOfCellOff])

		KeySize := binary.BigEndian.Uint16(b[Offset : Offset+SizeOfCellKeySize])
		c := Cell{
			Key: b[Offset+SizeOfCellKeySize : Offset+SizeOfCellKeySize+KeySize],
		}
		if p.Type == LeafPage {
			ValueSize := binary.BigEndian.Uint16(b[Offset+SizeOfCellKeySize+KeySize : Offset+SizeOfCellKeySize+KeySize+SizeOfCellValueSize])
			c.Value = b[Offset+SizeOfCellKeySize+KeySize+SizeOfCellValueSize : Offset+SizeOfCellKeySize+KeySize+SizeOfCellValueSize+ValueSize]
		} else {
			c.Value = b[Offset+SizeOfCellKeySize+KeySize : Offset+SizeOfCellKeySize+KeySize+4] // uint32 page no.
		}

		p.CellPtrArr = append(p.CellPtrArr, Offset)
		p.Cells[Offset] = c
	}

	return p
}

func Uint32ToBytes(ptr uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, ptr)
	return b
}

func BytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
