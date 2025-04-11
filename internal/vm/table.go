package vm

import (
	"errors"
	"strconv"
	"strings"

	"github.com/m0hossam/rocketsql/internal/storage"
)

type TableScan struct {
	tblName  string
	btree    *storage.Btree
	it       *storage.Iterator
	colNames []string          // TODO: store colTypes for type validation
	row      map[string]string // col names -> col values

}

func NewTableScan(tblName string, btree *storage.Btree) *TableScan {
	return &TableScan{
		tblName:  tblName,
		btree:    btree,
		it:       nil,
		colNames: []string{},
		row:      make(map[string]string),
	}
}

func (ts *TableScan) Init() error {
	rootPgNo, colNames, _, err := ts.getTableMetaData(ts.tblName)
	if err != nil {
		return err
	}

	ts.it = ts.btree.BtreeFirst(rootPgNo)
	ts.colNames = colNames

	return nil
}

func (ts *TableScan) Next() (bool, error) {
	if ts.it == nil {
		return false, errors.New("iterator is not initialized")
	}

	row, isNotEnd, err := ts.it.Next()
	if err != nil {
		return false, err
	}

	colVals := strings.Split(row, "|") // TODO: if a colVal contains '|', this will lead to an index out of bounds below
	for i, colVal := range colVals {
		ts.row[ts.colNames[i]] = colVal
	}

	return isNotEnd, nil
}

func (ts *TableScan) GetInt16(colName string) (int16, error) {
	if !ts.HasColumn(colName) {
		return 0, errors.New("column not found")
	}

	val, err := strconv.Atoi(ts.row[colName])
	if err != nil {
		return 0, err
	}
	return int16(val), nil
}

func (ts *TableScan) GetInt32(colName string) (int32, error) {
	if !ts.HasColumn(colName) {
		return 0, errors.New("column not found")
	}

	val, err := strconv.Atoi(ts.row[colName])
	if err != nil {
		return 0, err
	}
	return int32(val), nil
}

func (ts *TableScan) GetInt64(colName string) (int64, error) {
	if !ts.HasColumn(colName) {
		return 0, errors.New("column not found")
	}

	val, err := strconv.Atoi(ts.row[colName])
	if err != nil {
		return 0, err
	}
	return int64(val), nil
}

func (ts *TableScan) GetFloat32(colName string) (float32, error) {
	if !ts.HasColumn(colName) {
		return 0, errors.New("column not found")
	}

	val, err := strconv.ParseFloat(ts.row[colName], 32)
	if err != nil {
		return 0, err
	}
	return float32(val), nil
}

func (ts *TableScan) GetFloat64(colName string) (float64, error) {
	if !ts.HasColumn(colName) {
		return 0, errors.New("column not found")
	}

	val, err := strconv.ParseFloat(ts.row[colName], 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (ts *TableScan) GetString(colName string) (string, error) {
	if !ts.HasColumn(colName) {
		return "", errors.New("column not found")
	}

	return ts.row[colName], nil
}

func (ts *TableScan) HasColumn(colName string) bool {
	for _, col := range ts.colNames {
		if col == colName {
			return true
		}
	}
	return false
}

func (ts *TableScan) Close() {
	ts.btree = nil
	ts.it = nil
	ts.colNames = nil
	ts.row = nil
}

// returns root page number, column names, column types
func (ts *TableScan) getTableMetaData(tblName string) (uint32, []string, []string, error) {
	serKey := storage.SerializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow, pg := ts.btree.BtreeGet(serKey, 1)
	if pg == storage.DbNullPage {
		return 0, nil, nil, errors.New("did not find table in master table")
	}

	line := storage.DeserializeRow(serRow)
	cols := strings.Split(line, "|") // split row into 3 columns (table name, root page no., schema)
	rootPageNo, _ := strconv.Atoi(cols[1])
	tokens := strings.Split(cols[2], " ") // split schema into tokens formatted like (col1 type1 col2 type2 ...)

	colNames := []string{}
	colTypes := []string{}

	for i := 0; i < len(tokens); i += 2 {
		colNames = append(colNames, tokens[i])
		colTypes = append(colTypes, tokens[i+1])
	}

	return uint32(rootPageNo), colNames, colTypes, nil
}
