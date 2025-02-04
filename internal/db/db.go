package db

import (
	"errors"
	"strconv"
	"strings"

	"github.com/m0hossam/rocketsql/internal/storage"
)

type Db struct {
	Path  string
	Btree *storage.Btree
	Pgr   *storage.Pager
}

func CreateDb(name string) (*Db, error) {
	db := &Db{}
	err := db.initDb(name)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// TODO: OpenDb()

func (db *Db) initDb(name string) error {
	db.Path = name + ".rocketsql"
	db.Pgr = storage.CreatePager(db.Path, 5) // TODO: adjust max frames here
	db.Btree = storage.CreateBtree(db.Pgr)

	err := storage.CreateDb(db.Path, db.Btree)
	if err != nil {
		return err
	}

	return nil
}

func (db *Db) CreateTable(tblName string, colNames []string, colTypes []string) error {
	rootPageNo, err := storage.GetFirstFreePagePtr(storage.DbFilePath)
	if err != nil {
		return err
	}

	sql := ""
	for idx, colName := range colNames {
		sql += colName + " " + strings.ToUpper(colTypes[idx]) + " "
	}
	sql = strings.Trim(sql, " ")

	// table name - root page no. - schema
	serKey := storage.SerializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow := storage.SerializeRow([]string{"VARCHAR(255)", "INT", "VARCHAR(255)"}, []string{tblName, strconv.Itoa(int(*rootPageNo)), sql})

	tblRootPg, err := storage.CreatePage(storage.LeafPage, rootPageNo)
	if err != nil {
		return err
	}
	err = db.Pgr.SaveNewPage(tblRootPg)
	if err != nil {
		return err
	}

	pg1, err := db.Pgr.LoadPage(1)
	if err != nil {
		return err
	}

	err = db.Btree.BtreeInsert(pg1, serKey, serRow, rootPageNo)
	if err != nil {
		return err
	}

	return nil
}

func (db *Db) GetTableMetaData(tblName string) (uint32, []string, []string, error) { // root page no., col names, col types
	pg1, err := db.Pgr.LoadPage(1)
	if err != nil {
		return 0, nil, nil, err
	}

	serKey := storage.SerializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow, pg := db.Btree.BtreeGet(serKey, pg1)
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

func (db *Db) GetRow(tblName string, primaryKey string) (string, error) {
	rootPgNo, _, colTypes, err := db.GetTableMetaData(tblName)
	if err != nil {
		return "", err
	}

	primaryKeyType := colTypes[0]
	serKey := storage.SerializeRow([]string{primaryKeyType}, []string{primaryKey})

	rootPg, err := db.Pgr.LoadPage(rootPgNo)
	if err != nil {
		return "", err
	}

	serRow, pg := db.Btree.BtreeGet(serKey, rootPg)
	if pg == storage.DbNullPage {
		return "", errors.New("did not find key in table")
	}

	return storage.DeserializeRow(serRow), nil
}

func (db *Db) InsertIntoTable(tblName string, colVals []string) error {
	rootPgNo, _, colTypes, err := db.GetTableMetaData(tblName)
	if err != nil {
		return err
	}

	serKey := storage.SerializeRow([]string{colTypes[0]}, []string{colVals[0]})
	serRow := storage.SerializeRow(colTypes, colVals)

	firstFreePtr, err := storage.GetFirstFreePagePtr(storage.DbFilePath)
	if err != nil {
		return err
	}

	rootPg, err := db.Pgr.LoadPage(rootPgNo)
	if err != nil {
		return err
	}

	err = db.Btree.BtreeInsert(rootPg, serKey, serRow, firstFreePtr)
	if err != nil {
		return err
	}

	return nil
}

func (db *Db) DeleteFromTable(tblName string, primaryKey string) error {
	rootPgNo, _, colTypes, err := db.GetTableMetaData(tblName)
	if err != nil {
		return err
	}

	primaryKeyType := colTypes[0]
	serKey := storage.SerializeRow([]string{primaryKeyType}, []string{primaryKey})

	firstFreePtr, err := storage.GetFirstFreePagePtr(storage.DbFilePath)
	if err != nil {
		return err
	}

	rootPg, err := db.Pgr.LoadPage(rootPgNo)
	if err != nil {
		return err
	}

	err = db.Btree.BtreeDelete(rootPg, serKey, firstFreePtr)
	if err != nil {
		return err
	}

	return nil
}
