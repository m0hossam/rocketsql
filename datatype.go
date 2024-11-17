package main

import (
	"encoding/binary"
	"math"
)

const (
	sqlNull     = 0
	sqlSmallint = 1
	sqlInt      = 2
	sqlBigint   = 3
	sqlFloat    = 4
	sqlDouble   = 5
	sqlChar     = 6
	sqlVarchar  = 7
)

const (
	firstLessThanSecond    = -1
	firstEqualSecond       = 0
	firstGreaterThanSecond = 1
)

func intCompare(a []byte, b []byte, datatype uint8) int {
	var ax, bx int64

	switch datatype {
	case sqlSmallint:
		ax = int64(int16(binary.BigEndian.Uint16(a)))
		bx = int64(int16(binary.BigEndian.Uint16(b)))
	case sqlInt:
		ax = int64(int32(binary.BigEndian.Uint32(a)))
		bx = int64(int32(binary.BigEndian.Uint32(b)))
	case sqlBigint:
		ax = int64(binary.BigEndian.Uint64(a))
		bx = int64(binary.BigEndian.Uint64(b))
	default:
		panic("Invalid datatype in intCompare()")
	}

	if ax < bx {
		return firstLessThanSecond
	}
	if ax > bx {
		return firstGreaterThanSecond
	}
	return firstEqualSecond
}

func floatCompare(a []byte, b []byte, datatype uint8) int {
	var ax, bx float64

	switch datatype {
	case sqlFloat:
		ax = float64(math.Float32frombits(binary.BigEndian.Uint32(a)))
		bx = float64(math.Float32frombits(binary.BigEndian.Uint32(b)))
	case sqlDouble:
		ax = math.Float64frombits(binary.BigEndian.Uint64(a))
		bx = math.Float64frombits(binary.BigEndian.Uint64(b))
	default:
		panic("Invalid datatype in floatCompare()")
	}

	if ax < bx {
		return firstLessThanSecond
	}
	if ax > bx {
		return firstGreaterThanSecond
	}
	return firstEqualSecond
}

func strCompare(a []byte, b []byte) int {
	as := string(a)
	bs := string(b)

	if as < bs {
		return firstLessThanSecond
	}
	if as > bs {
		return firstGreaterThanSecond
	}

	return firstEqualSecond
}

/*
assume both keys have same number of fields
and same datatypes in the same order
which should be true inside the same table
*/
func compare(a []byte, b []byte) int {
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
			return firstLessThanSecond
		}
		if aType > bType {
			return firstGreaterThanSecond
		}

		var res int
		switch aType { // aType == bType
		case sqlNull:
			res = firstEqualSecond
		case sqlSmallint:
			size := 2
			res = intCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case sqlInt:
			size := 4
			res = intCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case sqlBigint:
			size := 8
			res = intCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case sqlFloat:
			size := 4
			res = floatCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case sqlDouble:
			size := 8
			res = floatCompare(a[aOffset:aOffset+size], b[bOffset:bOffset+size], aType)
			aOffset += size
			bOffset += size
		case sqlChar:
			aSize := int(binary.BigEndian.Uint16(a[aOffset : aOffset+2]))
			aOffset += 2
			bSize := int(binary.BigEndian.Uint16(b[bOffset : bOffset+2]))
			bOffset += 2
			res = strCompare(a[aOffset:aOffset+aSize], b[bOffset:bOffset+bSize])
			aOffset += aSize
			bOffset += bSize
		case sqlVarchar:
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

		if res != firstEqualSecond {
			return res
		}
	}
	return firstEqualSecond
}
