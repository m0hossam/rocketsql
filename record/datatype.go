package record

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
)

type DataType uint8

const (
	SqlNull     = 0
	SqlSmallint = 1
	SqlInt      = 2
	SqlBigint   = 3
	SqlFloat    = 4
	SqlDouble   = 5
	SqlChar     = 6
	SqlVarchar  = 7
)

const (
	FirstLessThanSecond    = -1
	FirstEqualSecond       = 0
	FirstGreaterThanSecond = 1
)

func (dt DataType) ToString() string {
	switch dt {
	case SqlNull:
		return "NULL"
	case SqlSmallint:
		return "SMALLINT"
	case SqlInt:
		return "INT"
	case SqlBigint:
		return "BIGINT"
	case SqlFloat:
		return "FLOAT"
	case SqlDouble:
		return "DOUBLE"
	case SqlChar:
		return "CHAR"
	case SqlVarchar:
		return "VARCHAR"
	default:
		return "UNKNOWN"
	}
}

func StringToDataType(s string) DataType {
	s = strings.ToUpper(s)
	switch s {
	case "NULL":
		return SqlNull
	case "SMALLINT":
		return SqlSmallint
	case "INT":
		return SqlInt
	case "BIGINT":
		return SqlBigint
	case "FLOAT":
		return SqlFloat
	case "DOUBLE":
		return SqlDouble
	default:
		if s[:4] == "CHAR" {
			return SqlChar
		}
		return SqlVarchar
	}
}

func intCompare(a []byte, b []byte, datatype uint8) int {
	var ax, bx int64

	switch datatype {
	case SqlSmallint:
		ax = int64(int16(binary.BigEndian.Uint16(a)))
		bx = int64(int16(binary.BigEndian.Uint16(b)))
	case SqlInt:
		ax = int64(int32(binary.BigEndian.Uint32(a)))
		bx = int64(int32(binary.BigEndian.Uint32(b)))
	case SqlBigint:
		ax = int64(binary.BigEndian.Uint64(a))
		bx = int64(binary.BigEndian.Uint64(b))
	default:
		panic("Invalid datatype in intCompare()")
	}

	if ax < bx {
		return FirstLessThanSecond
	}
	if ax > bx {
		return FirstGreaterThanSecond
	}
	return FirstEqualSecond
}

func floatCompare(a []byte, b []byte, datatype uint8) int {
	var ax, bx float64

	switch datatype {
	case SqlFloat:
		ax = float64(math.Float32frombits(binary.BigEndian.Uint32(a)))
		bx = float64(math.Float32frombits(binary.BigEndian.Uint32(b)))
	case SqlDouble:
		ax = math.Float64frombits(binary.BigEndian.Uint64(a))
		bx = math.Float64frombits(binary.BigEndian.Uint64(b))
	default:
		panic("Invalid datatype in floatCompare()")
	}

	if ax < bx {
		return FirstLessThanSecond
	}
	if ax > bx {
		return FirstGreaterThanSecond
	}
	return FirstEqualSecond
}

func strCompare(a []byte, b []byte) int {
	as := string(a)
	bs := string(b)

	if as < bs {
		return FirstLessThanSecond
	}
	if as > bs {
		return FirstGreaterThanSecond
	}

	return FirstEqualSecond
}

func Compare(a []byte, b []byte) int {
	/*
		assume both keys have same number of fields
		and same datatypes in the same order
		which should be true inside the same table
	*/

	nFields := uint8(a[0])
	if nFields != uint8(b[0]) {
		panic("Keys have different number of fields")
	}

	aOffset := int(1 + nFields)
	bOffset := int(1 + nFields)
	for i := 0; i < int(nFields); i++ {
		aType := uint8(a[1+i])
		bType := uint8(b[1+i])

		// the following two conditions should only happen when one of the fields is NULL and the other isn't
		if aType < bType {
			return FirstLessThanSecond
		}
		if aType > bType {
			return FirstGreaterThanSecond
		}

		var res int
		switch aType { // aType == bType
		case SqlNull:
			res = FirstEqualSecond
		case SqlSmallint:
			size := 2
			res = intCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case SqlInt:
			size := 4
			res = intCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case SqlBigint:
			size := 8
			res = intCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case SqlFloat:
			size := 4
			res = floatCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case SqlDouble:
			size := 8
			res = floatCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case SqlChar:
			aSize := int(binary.BigEndian.Uint16(a[aOffset : aOffset+2]))
			aOffset += 2
			bSize := int(binary.BigEndian.Uint16(b[bOffset : bOffset+2]))
			bOffset += 2
			res = strCompare(a[aOffset:aOffset+aSize], b[bOffset:bOffset+bSize])
			aOffset += aSize
			bOffset += bSize
		case SqlVarchar:
			aSize := int(binary.BigEndian.Uint16(a[aOffset : aOffset+2]))
			aOffset += 2
			bSize := int(binary.BigEndian.Uint16(b[bOffset : bOffset+2]))
			bOffset += 2
			res = strCompare(a[aOffset:aOffset+aSize], b[bOffset:bOffset+bSize])
			aOffset += aSize
			bOffset += bSize
		default:
			panic("Invalid datatype")
		}

		if res != FirstEqualSecond {
			return res
		}
	}
	return FirstEqualSecond
}

/*
#################################################################################################
#################################################################################################
TODO: REMOVE EVERYTHING BELOW THIS BLOCK ########################################################
#################################################################################################
#################################################################################################
*/

func SerializeRow(colTypes []string, colVals []string) []byte {
	b := []byte{}
	nFields := len(colTypes)
	b = append(b, uint8(nFields))

	for _, colType := range colTypes {
		b = append(b, uint8(StringToDataType(colType)))
	}

	idx := 0
	for _, colTypeStr := range colTypes {
		colType := StringToDataType(colTypeStr)
		if colType == SqlNull {
			continue
		}

		colVal := colVals[idx]
		idx++

		switch colType {
		case SqlSmallint:
			i, _ := strconv.ParseInt(colVal, 10, 16)
			num := int16(i)
			buf := make([]byte, 2)
			binary.BigEndian.PutUint16(buf, uint16(num))
			b = append(b, buf...)
		case SqlInt:
			i, _ := strconv.ParseInt(colVal, 10, 32)
			num := int32(i)
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, uint32(num))
			b = append(b, buf...)
		case SqlBigint:
			i, _ := strconv.ParseInt(colVal, 10, 64)
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(i))
			b = append(b, buf...)
		case SqlFloat:
			f, _ := strconv.ParseFloat(colVal, 32)
			num := float32(f)
			buf := make([]byte, 4)
			bits := math.Float32bits(num)
			binary.BigEndian.PutUint32(buf, bits)
			b = append(b, buf...)
		case SqlDouble:
			f, _ := strconv.ParseFloat(colVal, 64)
			num := float64(f)
			buf := make([]byte, 8)
			bits := math.Float64bits(num)
			binary.BigEndian.PutUint64(buf, bits)
			b = append(b, buf...)

		case SqlChar:
			buf := []byte(colVal)
			sz := uint16(len(buf))
			szBuf := make([]byte, 2)
			binary.BigEndian.PutUint16(szBuf, sz)
			b = append(b, szBuf...)
			b = append(b, buf...)
		case SqlVarchar:
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
		case SqlNull:
			res += "NULL"
		case SqlSmallint:
			size := 2
			num := int16(binary.BigEndian.Uint16(row[off : off+size]))
			res += strconv.Itoa(int(num))
			off += size
		case SqlInt:
			size := 4
			num := int32(binary.BigEndian.Uint32(row[off : off+size]))
			res += strconv.Itoa(int(num))
			off += size
		case SqlBigint:
			size := 8
			num := int64(binary.BigEndian.Uint64(row[off : off+size]))
			res += strconv.Itoa(int(num))
			off += size
		case SqlFloat:
			size := 4
			num := math.Float32frombits(binary.BigEndian.Uint32(row[off : off+size]))
			res += strconv.FormatFloat(float64(num), 'f', -1, 32)
			off += size
		case SqlDouble:
			size := 8
			num := math.Float64frombits(binary.BigEndian.Uint64(row[off : off+size]))
			res += strconv.FormatFloat(num, 'f', -1, 64)
			off += size
		case SqlChar:
			size := int(binary.BigEndian.Uint16(row[off : off+2]))
			off += 2
			res += string(row[off : off+size])
			off += size
		case SqlVarchar:
			size := int(binary.BigEndian.Uint16(row[off : off+2]))
			off += 2
			res += string(row[off : off+size])
			off += size
		}
		if i != int(nFields)-1 {
			res += "|"
		}
	}
	return strings.Trim(res, " ")
}
