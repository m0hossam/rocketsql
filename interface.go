package main

import (
	"encoding/binary"
	"errors"
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

func searchTable(tblName string, primaryKey string) (string, error) {
	pg1, err := loadPage(1)
	if err != nil {
		return "", err
	}

	serKey := serializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow, pg := find(serKey, pg1)
	if pg == dbNullPage {
		return "", errors.New("did not find table in master table")
	}

	line := deserializeRow(serRow)
	tokens := strings.Split(line, " ")
	primaryKeyType := tokens[3]
	serKey = serializeRow([]string{primaryKeyType}, []string{primaryKey})

	num, _ := strconv.Atoi(tokens[1])
	rootPgNo := uint32(num)
	rootPg, err := loadPage(rootPgNo)
	if err != nil {
		return "", err
	}

	serRow, pg = find(serKey, rootPg)
	if pg == dbNullPage {
		return "", errors.New("did not find key in table")
	}

	return deserializeRow(serRow), nil
}

func insertIntoTable(tblName string, colTypes []string, colVals []string) error {
	pg1, err := loadPage(1)
	if err != nil {
		return err
	}

	serKey := serializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow, pg := find(serKey, pg1)
	if pg == dbNullPage {
		return errors.New("did not find table in master table")
	}

	line := deserializeRow(serRow)
	num, _ := strconv.Atoi(strings.Split(line, " ")[1])
	rootPgNo := uint32(num)

	serKey = serializeRow([]string{colTypes[0]}, []string{colVals[0]})
	serRow = serializeRow(colTypes, colVals)

	firstFreePtr, err := getFirstFreePagePtr(dbFilePath)
	if err != nil {
		return err
	}

	rootPg, err := loadPage(rootPgNo)
	if err != nil {
		return err
	}

	err = insert(rootPg, firstFreePtr, serKey, serRow, true, nil, dbNullPage, dbNullPage)
	if err != nil {
		return err
	}

	return nil
}

func createTable(tblName string, colNames []string, colTypes []string) error {
	rootPageNo, err := getFirstFreePagePtr(dbFilePath)
	if err != nil {
		return err
	}

	sql := ""
	for idx, colName := range colNames {
		sql += colName + " " + strings.ToUpper(colTypes[idx]) + " "
	}
	sql = strings.Trim(sql, " ")

	serKey := serializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow := serializeRow([]string{"VARCHAR(255)", "INT", "VARCHAR(255)"}, []string{tblName, strconv.Itoa(int(*rootPageNo)), sql})

	tblRootPg, err := createPage(leafPage, rootPageNo)
	if err != nil {
		return err
	}

	pg1, err := loadPage(1)
	if err != nil {
		return err
	}

	err = insert(pg1, rootPageNo, serKey, serRow, true, nil, dbNullPage, dbNullPage)
	if err != nil {
		return err
	}
	pg1 = nil

	err = saveNewPage(tblRootPg)
	if err != nil {
		return err
	}

	return nil
}
