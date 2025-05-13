package record

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/m0hossam/rocketsql/parser"
)

type Record struct {
	Columns []*parser.TypeDef
	Values  []*parser.Constant
}

func NewRecord(data []byte) (*Record, error) {
	nFields := uint8(data[0])
	columns := make([]*parser.TypeDef, nFields)
	values := make([]*parser.Constant, nFields)
	off := int(1 + nFields)
	for i := 0; i < int(nFields); i++ {
		colType := uint8(data[1+i])
		switch colType {
		case SqlNull:
			columns[i] = &parser.TypeDef{Type: "NULL"}
		case SqlSmallint:
			size := 2
			num := int16(binary.BigEndian.Uint16(data[off : off+size]))
			columns[i] = &parser.TypeDef{Type: "SMALLINT"}
			values[i] = &parser.Constant{IntVal: int64(num)}
			off += size
		case SqlInt:
			size := 4
			num := int32(binary.BigEndian.Uint32(data[off : off+size]))
			columns[i] = &parser.TypeDef{Type: "INT"}
			values[i] = &parser.Constant{IntVal: int64(num)}
			off += size
		case SqlBigint:
			size := 8
			num := int64(binary.BigEndian.Uint64(data[off : off+size]))
			columns[i] = &parser.TypeDef{Type: "BIGINT"}
			values[i] = &parser.Constant{IntVal: int64(num)}
			off += size
		case SqlFloat:
			size := 4
			num := math.Float32frombits(binary.BigEndian.Uint32(data[off : off+size]))
			columns[i] = &parser.TypeDef{Type: "FLOAT"}
			values[i] = &parser.Constant{FloatVal: float64(num)}
			off += size
		case SqlDouble:
			size := 8
			num := math.Float64frombits(binary.BigEndian.Uint64(data[off : off+size]))
			columns[i] = &parser.TypeDef{Type: "FLOAT"}
			values[i] = &parser.Constant{FloatVal: num}
			off += size
		case SqlChar:
			size := int(binary.BigEndian.Uint16(data[off : off+2]))
			off += 2
			columns[i] = &parser.TypeDef{Type: "CHAR", Size: size}
			values[i] = &parser.Constant{StrVal: string(data[off : off+size])}
			off += size
		case SqlVarchar:
			size := int(binary.BigEndian.Uint16(data[off : off+2]))
			off += 2
			columns[i] = &parser.TypeDef{Type: "CHAR", Size: size}
			values[i] = &parser.Constant{StrVal: string(data[off : off+size])}
			off += size
		default:
			return nil, errors.New("unknown type")
		}
	}
	return &Record{Columns: columns, Values: values}, nil
}

func (r *Record) Serialize() ([]byte, error) {
	nFields := len(r.Columns)

	if nFields <= 0 {
		return nil, errors.New("no fields")
	}
	if nFields != len(r.Values) {
		return nil, errors.New("number of fields and values do not match")
	}

	b := make([]byte, 0)
	b = append(b, uint8(nFields))

	for _, typeDef := range r.Columns {
		b = append(b, uint8(StringToDataType(typeDef.Type)))
	}

	i := 0
	for _, typeDef := range r.Columns {
		if typeDef.Type == "NULL" {
			continue
		}

		curValue := r.Values[i]
		i++

		switch typeDef.Type {
		case "SMALLINT":
			num := int16(curValue.IntVal)
			buf := make([]byte, 2)
			binary.BigEndian.PutUint16(buf, uint16(num))
			b = append(b, buf...)
		case "INT":
			num := int32(curValue.IntVal)
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, uint32(num))
			b = append(b, buf...)
		case "BIGINT":
			num := int64(curValue.IntVal)
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(num))
			b = append(b, buf...)
		case "FLOAT":
			num := float32(curValue.FloatVal)
			buf := make([]byte, 4)
			bits := math.Float32bits(num)
			binary.BigEndian.PutUint32(buf, bits)
			b = append(b, buf...)
		case "DOUBLE":
			num := curValue.FloatVal
			buf := make([]byte, 8)
			bits := math.Float64bits(num)
			binary.BigEndian.PutUint64(buf, bits)
			b = append(b, buf...)
		case "CHAR":
			sz := uint16(typeDef.Size)
			buf := []byte(curValue.StrVal)
			actualSz := uint16(len(buf))
			if actualSz > sz {
				return nil, errors.New("char max size exceeded")
			}
			if actualSz < sz {
				buf = append(buf, make([]byte, sz-actualSz)...)
			}
			szBuf := make([]byte, 2)
			binary.BigEndian.PutUint16(szBuf, sz)
			b = append(b, szBuf...)
			b = append(b, buf...)
		case "VARCHAR":
			buf := []byte(curValue.StrVal)
			sz := uint16(len(buf))
			if sz > uint16(typeDef.Size) {
				return nil, errors.New("varchar max size exceeded")
			}
			szBuf := make([]byte, 2)
			binary.BigEndian.PutUint16(szBuf, sz)
			b = append(b, szBuf...)
			b = append(b, buf...)
		}
	}

	return b, nil
}
