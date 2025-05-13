package processor

import (
	"errors"

	"github.com/m0hossam/rocketsql/btree"
	"github.com/m0hossam/rocketsql/metadata"
	"github.com/m0hossam/rocketsql/parser"
	"github.com/m0hossam/rocketsql/record"
)

type TableScan struct {
	metadata    *metadata.TableMetadata
	btree       *btree.Btree
	btreeIt     *btree.BtreeIterator
	curRecord   *record.Record
	columnIndex map[string]int
}

func NewTableScan(metadata *metadata.TableMetadata, btree *btree.Btree) *TableScan {
	ts := &TableScan{
		metadata:    metadata,
		btree:       btree,
		columnIndex: make(map[string]int),
	}
	for i, fieldDef := range ts.metadata.TableSchema.FieldDefs {
		ts.columnIndex[fieldDef.Name] = i
	}
	return ts
}

func (ts *TableScan) BeforeFirst() error {
	it, err := ts.btree.First(ts.metadata.RootPageNo)
	if err != nil {
		return err
	}
	ts.btreeIt = it
	ts.curRecord = nil
	return nil
}

func (ts *TableScan) Next() (bool, error) {
	serializedRecord, next, err := ts.btreeIt.Next()
	if err != nil {
		return false, err
	}
	record, err := record.NewRecord(serializedRecord)
	if err != nil {
		return false, err
	}
	ts.curRecord = record
	return next, nil
}

func (ts *TableScan) GetInt16(colName string) (int16, error) {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return 0, errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "SMALLINT" {
		return 0, errors.New("column type mismatch")
	}

	return int16(ts.curRecord.Values[i].IntVal), nil
}

func (ts *TableScan) GetInt32(colName string) (int32, error) {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return 0, errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "INT" && ts.curRecord.Columns[i].Type != "SMALLINT" { // Allow casting from int16
		return 0, errors.New("column type mismatch")
	}

	return int32(ts.curRecord.Values[i].IntVal), nil
}

func (ts *TableScan) GetInt64(colName string) (int64, error) {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return 0, errors.New("no column with this name")
	}

	// Allow casting from int16 & int32
	if ts.curRecord.Columns[i].Type != "BIGINT" &&
		ts.curRecord.Columns[i].Type != "INT" &&
		ts.curRecord.Columns[i].Type != "SMALLINT" {
		return 0, errors.New("column type mismatch")
	}

	return ts.curRecord.Values[i].IntVal, nil
}

func (ts *TableScan) GetFloat32(colName string) (float32, error) {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return 0, errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "FLOAT" {
		return 0, errors.New("column type mismatch")
	}

	return float32(ts.curRecord.Values[i].FloatVal), nil
}

func (ts *TableScan) GetFloat64(colName string) (float64, error) {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return 0, errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "DOUBLE" && ts.curRecord.Columns[i].Type != "FLOAT" { // Allow casting from float32
		return 0, errors.New("column type mismatch")
	}

	return ts.curRecord.Values[i].FloatVal, nil
}

func (ts *TableScan) GetString(colName string) (string, error) {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return "", errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "CHAR" && ts.curRecord.Columns[i].Type != "VARCHAR" {
		return "", errors.New("column type mismatch")
	}

	return ts.curRecord.Values[i].StrVal, nil
}

func (ts *TableScan) GetType(colName string) (string, error) {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return "", errors.New("no column with this name")
	}

	return ts.curRecord.Columns[i].Type, nil
}

func (ts *TableScan) HasColumn(colName string) bool {
	_, ok := ts.columnIndex[colName]
	return ok
}

func (ts *TableScan) SetInt16(colName string, val int16) error {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return errors.New("no column with this name")
	}

	// Allow casting to int32 & int64
	if ts.curRecord.Columns[i].Type != "BIGINT" &&
		ts.curRecord.Columns[i].Type != "INT" &&
		ts.curRecord.Columns[i].Type != "SMALLINT" {
		return errors.New("column type mismatch")
	}

	ts.curRecord.Values[i].IntVal = int64(val)
	return nil
}

func (ts *TableScan) SetInt32(colName string, val int32) error {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "INT" && ts.curRecord.Columns[i].Type != "BIGINT" { // Allow casting to int64
		return errors.New("column type mismatch")
	}

	ts.curRecord.Values[i].IntVal = int64(val)
	return nil
}

func (ts *TableScan) SetInt64(colName string, val int64) error {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "BIGINT" {
		return errors.New("column type mismatch")
	}

	ts.curRecord.Values[i].IntVal = int64(val)
	return nil
}

func (ts *TableScan) SetFloat32(colName string, val float32) error {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "FLOAT" && ts.curRecord.Columns[i].Type != "DOUBLE" { // Allow casting to float64
		return errors.New("column type mismatch")
	}

	ts.curRecord.Values[i].FloatVal = float64(val)
	return nil
}

func (ts *TableScan) SetFloat64(colName string, val float64) error {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "DOUBLE" {
		return errors.New("column type mismatch")
	}

	ts.curRecord.Values[i].FloatVal = val
	return nil
}

func (ts *TableScan) SetString(colName string, val string) error {
	i, columnExists := ts.columnIndex[colName]
	if !columnExists {
		return errors.New("no column with this name")
	}

	if ts.curRecord.Columns[i].Type != "CHAR" && ts.curRecord.Columns[i].Type != "VARCHAR" {
		return errors.New("column type mismatch")
	}

	ts.curRecord.Values[i].StrVal = val
	return nil
}

func (ts *TableScan) InsertRow() error {
	// Do not use GetRowKey() because this might be part of an update operation and the key could be modified
	keyRec := &record.Record{
		Columns: []*parser.TypeDef{ts.curRecord.Columns[0]},
		Values:  []*parser.Constant{ts.curRecord.Values[0]},
	}

	key, err := keyRec.Serialize()
	if err != nil {
		return err
	}

	value, err := ts.curRecord.Serialize()
	if err != nil {
		return err
	}

	return ts.btree.Insert(ts.metadata.RootPageNo, key, value)
}

func (ts *TableScan) DeleteRow() error {
	return ts.btree.Delete(ts.metadata.RootPageNo, ts.GetRowKey())
}

func (ts *TableScan) GetRowKey() []byte {
	return ts.btreeIt.GetKey()
}
