package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Toy interface for testing purposes, will be removed later

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

func deserializeRow(row []byte) string {
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

func serializeRow(colTypes []string, colVals []string) []byte {
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

func insertIntoTable(tblName string, colNameType map[string]string, colNameVal map[string]string) {

}

func createTable(tblName string, colNameType map[string]string) {
	rootPageNo, err := getFirstFreePagePtr(dbFilePath)
	if err != nil {
		panic(err)
	}
	sql := ""
	for colName, colType := range colNameType {
		sql += colName + " " + colType + " "
	}
	sql = strings.Trim(sql, " ")
	fmt.Println(*rootPageNo)
}
