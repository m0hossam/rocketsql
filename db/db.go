package db

import (
	"github.com/m0hossam/rocketsql/btree"
	"github.com/m0hossam/rocketsql/parser"
	"github.com/m0hossam/rocketsql/processor"
)

type Db struct {
	dbFilePath string
	btree      *btree.Btree
	processor  *processor.Processor
}

func NewDb(dbFilePath string) (*Db, error) {
	btree, err := btree.NewBtree(dbFilePath)
	if err != nil {
		return nil, err
	}

	return &Db{
		dbFilePath: dbFilePath,
		btree:      btree,
		processor:  processor.NewProcessor(btree),
	}, nil
}

// rows affected, result table
func (db *Db) ExecuteSQL(sql string) (int, processor.Scan, error) {
	p := parser.NewParser(sql)
	pt, err := p.Parse()
	if err != nil {
		return 0, nil, err
	}

	return db.processor.ExecuteSQL(pt)
}

func (db *Db) Close() error {
	if db.btree != nil {
		return db.btree.Close()
	}
	return nil
}
