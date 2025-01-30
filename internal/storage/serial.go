package storage

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
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
		if p.pType == LeafPage { // for interior pages, cell.value is a uint32 page no.
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
		if p.pType == LeafPage {
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

func getDatatype(s string) int {
	s = strings.ToUpper(s)
	switch s {
	case "NULL":
		return sqlNull
	case "SMALLINT":
		return sqlSmallint
	case "INT":
		return sqlInt
	case "BIGINT":
		return sqlBigint
	case "FLOAT":
		return sqlFloat
	case "DOUBLE":
		return sqlDouble
	default:
		if s[:4] == "CHAR" {
			return sqlChar
		}
		return sqlVarchar
	}
}

func SerializeRow(colTypes []string, colVals []string) []byte {
	b := []byte{}
	nFields := len(colTypes)
	b = append(b, uint8(nFields))

	for _, colType := range colTypes {
		b = append(b, uint8(getDatatype(colType)))
	}

	idx := 0
	for _, colTypeStr := range colTypes {
		colType := getDatatype(colTypeStr)
		if colType == sqlNull {
			continue
		}

		colVal := colVals[idx]
		idx++

		switch colType {
		case sqlSmallint:
			i, _ := strconv.ParseInt(colVal, 10, 16)
			num := int16(i)
			buf := make([]byte, 2)
			binary.BigEndian.PutUint16(buf, uint16(num))
			b = append(b, buf...)
		case sqlInt:
			i, _ := strconv.ParseInt(colVal, 10, 32)
			num := int32(i)
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, uint32(num))
			b = append(b, buf...)
		case sqlBigint:
			i, _ := strconv.ParseInt(colVal, 10, 64)
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(i))
			b = append(b, buf...)
		case sqlFloat:
			f, _ := strconv.ParseFloat(colVal, 32)
			num := float32(f)
			buf := make([]byte, 4)
			bits := math.Float32bits(num)
			binary.BigEndian.PutUint32(buf, bits)
			b = append(b, buf...)
		case sqlDouble:
			f, _ := strconv.ParseFloat(colVal, 64)
			num := float64(f)
			buf := make([]byte, 8)
			bits := math.Float64bits(num)
			binary.BigEndian.PutUint64(buf, bits)
			b = append(b, buf...)

		case sqlChar:
			buf := []byte(colVal)
			sz := uint16(len(buf))
			szBuf := make([]byte, 2)
			binary.BigEndian.PutUint16(szBuf, sz)
			b = append(b, szBuf...)
			b = append(b, buf...)
		case sqlVarchar:
			buf := []byte(colVal)
			sz := uint16(len(buf))
			szBuf := make([]byte, 2)
			binary.BigEndian.PutUint16(szBuf, sz)
			b = append(b, szBuf...)
			b = append(b, buf...)
		}
	}

	return b
}

func DeserializeRow(row []byte) string {
	res := ""
	nFields := uint8(row[0])
	off := int(1 + nFields)
	for i := 0; i < int(nFields); i++ {
		colType := uint8(row[1+i])
		switch colType {
		case sqlNull:
			res += "NULL"
		case sqlSmallint:
			size := 2
			num := int16(binary.BigEndian.Uint16(row[off : off+size]))
			res += strconv.Itoa(int(num))
			off += size
		case sqlInt:
			size := 4
			num := int32(binary.BigEndian.Uint32(row[off : off+size]))
			res += strconv.Itoa(int(num))
			off += size
		case sqlBigint:
			size := 8
			num := int64(binary.BigEndian.Uint64(row[off : off+size]))
			res += strconv.Itoa(int(num))
			off += size
		case sqlFloat:
			size := 4
			num := math.Float32frombits(binary.BigEndian.Uint32(row[off : off+size]))
			res += strconv.FormatFloat(float64(num), 'f', -1, 32)
			off += size
		case sqlDouble:
			size := 8
			num := math.Float64frombits(binary.BigEndian.Uint64(row[off : off+size]))
			res += strconv.FormatFloat(num, 'f', -1, 64)
			off += size
		case sqlChar:
			size := int(binary.BigEndian.Uint16(row[off : off+2]))
			off += 2
			res += string(row[off : off+size])
			off += size
		case sqlVarchar:
			size := int(binary.BigEndian.Uint16(row[off : off+2]))
			off += 2
			res += string(row[off : off+size])
			off += size
		}
		res += " "
	}
	return strings.Trim(res, " ")
}

func deserializePtr(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
