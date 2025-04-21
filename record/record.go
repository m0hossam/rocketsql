package record

import (
	"encoding/binary"
)

type Record struct {
	nFields uint8
	types   []DataType
	values  []*Value
}

func NewRecord(data []byte) *Record {
	nFields := data[0]
	types := make([]DataType, nFields)
	values := make([]*Value, nFields)
	off := int(1 + nFields)
	for i := 0; i < int(nFields); i++ {
		var size int
		colType := DataType(data[1+i])
		switch colType {
		case SqlNull:
			size = 0
		case SqlSmallint:
			size = 2
		case SqlInt:
			size = 4
		case SqlBigint:
			size = 8
		case SqlFloat:
			size = 4
		case SqlDouble:
			size = 8
		case SqlChar:
			size = int(binary.BigEndian.Uint16(data[off:off+2])) + 2
		case SqlVarchar:
			size = int(binary.BigEndian.Uint16(data[off:off+2])) + 2
		default:
			panic("Unknown data type")
		}
		types[i] = colType
		values[i] = NewValue(data[off:off+size], colType)
		off += size
	}

	return &Record{
		nFields: nFields,
		types:   types,
		values:  values,
	}
}

func (r *Record) Serialize() []byte {
	nFields := int(r.nFields)
	if (nFields != len(r.types)) || (nFields != len(r.values)) {
		panic("Invalid record")
	}

	buf := make([]byte, 1+r.nFields)

	buf[0] = r.nFields
	for i := 0; i < nFields; i++ {
		buf[i+1] = byte(r.types[i])
	}

	for i := 0; i < nFields; i++ {
		if r.types[i] != SqlNull {
			buf = append(buf, r.values[i].data...)
		}
	}

	return buf
}

func (r *Record) GetValue(i int) *Value {
	if i < 0 || i >= len(r.values) {
		panic("Index out of bounds")
	}
	return r.values[i]
}
