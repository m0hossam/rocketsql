package db

import (
	"errors"
	"strconv"
	"strings"

	"github.com/m0hossam/rocketsql/btree"
	"github.com/m0hossam/rocketsql/page"
	"github.com/m0hossam/rocketsql/record"
)

type Db struct {
	dbFilePath string
	btree      *btree.Btree
}

func NewDb(dbFilePath string) (*Db, error) {
	btree, err := btree.NewBtree(dbFilePath)
	if err != nil {
		return nil, err
	}

	return &Db{
		dbFilePath: dbFilePath,
		btree:      btree,
	}, nil
}

func (db *Db) Close() error {
	if db.btree != nil {
		return db.btree.Close()
	}
	return nil
}

/*
#################################################################################################
#################################################################################################
TODO: REMOVE EVERYTHING BELOW THIS BLOCK ########################################################
#################################################################################################
#################################################################################################
*/

func (db *Db) CreateTable(tblName string, colNames []string, colTypes []string) error {
	rootPageNo := *db.btree.GetNewPagePtr()

	sql := ""
	for idx, colName := range colNames {
		sql += colName + " " + strings.ToUpper(colTypes[idx]) + " "
	}
	sql = strings.Trim(sql, " ")

	// table name - root page no. - schema
	serKey := record.SerializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow := record.SerializeRow([]string{"VARCHAR(255)", "INT", "VARCHAR(255)"}, []string{tblName, strconv.Itoa(int(rootPageNo)), sql})

	return db.btree.Create(serKey, serRow)
}

// root page no., col names, col types
func (db *Db) GetTableMetaData(tblName string) (uint32, []string, []string, error) {
	serKey := record.SerializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow, pg := db.btree.Get(serKey, 1)
	if pg == page.DbNullPage {
		return 0, nil, nil, errors.New("did not find table in master table")
	}

	line := record.DeserializeRow(serRow)
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

func (db *Db) GetRow(tblName string, primaryKey string) (string, error) {
	rootPgNo, _, colTypes, err := db.GetTableMetaData(tblName)
	if err != nil {
		return "", err
	}

	primaryKeyType := colTypes[0]
	serKey := record.SerializeRow([]string{primaryKeyType}, []string{primaryKey})

	serRow, pg := db.btree.Get(serKey, rootPgNo)
	if pg == page.DbNullPage {
		return "", errors.New("did not find key in table")
	}

	return record.DeserializeRow(serRow), nil
}

func (db *Db) InsertRow(tblName string, colVals []string) error {
	rootPgNo, _, colTypes, err := db.GetTableMetaData(tblName)
	if err != nil {
		return err
	}

	serKey := record.SerializeRow([]string{colTypes[0]}, []string{colVals[0]})
	serRow := record.SerializeRow(colTypes, colVals)

	err = db.btree.Insert(rootPgNo, serKey, serRow)
	if err != nil {
		return err
	}

	return nil
}

func (db *Db) DeleteRow(tblName string, primaryKey string) error {
	rootPgNo, _, colTypes, err := db.GetTableMetaData(tblName)
	if err != nil {
		return err
	}

	primaryKeyType := colTypes[0]
	serKey := record.SerializeRow([]string{primaryKeyType}, []string{primaryKey})

	err = db.btree.Delete(rootPgNo, serKey)
	if err != nil {
		return err
	}

	return nil
}

func (db *Db) UpdateRow(tblName string, primaryKey string, newVals []string) error {
	rootPgNo, _, colTypes, err := db.GetTableMetaData(tblName)
	if err != nil {
		return err
	}

	primaryKeyType := colTypes[0]
	serKey := record.SerializeRow([]string{primaryKeyType}, []string{primaryKey})

	err = db.btree.Delete(rootPgNo, serKey)
	if err != nil {
		return err
	}

	return db.InsertRow(tblName, newVals)
}

func (db *Db) GetBtreeIterator(rootPgNo uint32) (*btree.BtreeIterator, error) {
	return db.btree.First(rootPgNo)
}
