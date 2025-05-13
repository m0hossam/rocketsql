package record

import (
	"encoding/binary"
	"math"
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
