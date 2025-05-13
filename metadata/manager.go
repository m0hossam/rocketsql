package metadata

import (
	"errors"

	"github.com/m0hossam/rocketsql/btree"
	"github.com/m0hossam/rocketsql/parser"
	"github.com/m0hossam/rocketsql/record"
)

type TableManager struct {
	btree *btree.Btree
}

type TableMetadata struct {
	RootPageNo  uint32
	TableSchema *parser.CreateTableData
}

func NewTableManager(bt *btree.Btree) *TableManager {
	return &TableManager{
		btree: bt,
	}
}

func (tm *TableManager) GetTableMetadata(tableName string) (*TableMetadata, error) {
	rec := &record.Record{
		Columns: []*parser.TypeDef{{Type: "VARCHAR", Size: 255}},
		Values:  []*parser.Constant{{StrVal: tableName}},
	}

	key, err := rec.Serialize()
	if err != nil {
		return nil, err
	}

	data, pgNo := tm.btree.Get(key, 1)
	if pgNo == 0 {
		return nil, errors.New("table not found")
	}

	rec, err = record.NewRecord(data)
	if err != nil {
		return nil, err
	}

	rootPageNo := uint32(rec.Values[1].IntVal)
	sqlSchema := rec.Values[2].StrVal

	p := parser.NewParser(sqlSchema)
	pt, err := p.Parse()
	if err != nil {
		return nil, err
	}

	return &TableMetadata{
		RootPageNo:  rootPageNo,
		TableSchema: pt.CreateTableData,
	}, nil
}
