package db

import (
	"fmt"
	"strconv"

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

func (db *Db) ExecuteMetaCommand(cmd string) string {
	// .dump_table <table name>
	if len(cmd) >= 13 {
		if cmd[:12] == ".dump_table " {
			tblName := cmd[12:]
			query := fmt.Sprintf("SELECT root_page_no FROM rocketsql_schema WHERE table_name='%s'", tblName)

			_, result, err := db.ExecuteSQL(query)
			if err != nil {
				return err.Error()
			}

			if err = result.BeforeFirst(); err != nil {
				return err.Error()
			}

			if _, err = result.Next(); err != nil {
				return err.Error()
			}

			i32, err := result.GetInt32("root_page_no")
			if err != nil {
				return err.Error()
			}

			return db.btree.DumpBTree(tblName, uint32(i32))
		}
	}

	// .dump_page <page number>
	if len(cmd) >= 12 {
		if cmd[:11] == ".dump_page " {
			pgNum, err := strconv.ParseInt(cmd[11:], 10, 32)
			if err != nil {
				return err.Error()
			}

			return db.btree.DumpBTreePage(uint32(pgNum))
		}
	}

	return ""
}

func (db *Db) Close() error {
	if db.btree != nil {
		return db.btree.Close()
	}
	return nil
}
