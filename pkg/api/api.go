package api

import (
	"errors"
	"strconv"
	"strings"

	"github.com/m0hossam/rocketsql/internal/storage"
)

// Toy interface for testing purposes, will be removed later

func CreateDB(path string) error {
	err := storage.CreateDB(path)
	if err != nil {
		return err
	}

	return nil
}

func OpenDB(path string) error {
	err := storage.OpenDB(path)
	if err != nil {
		return err
	}

	return nil
}

func SearchTable(tblName string, primaryKey string) (string, error) {
	pg1, err := storage.LoadPage(1)
	if err != nil {
		return "", err
	}

	serKey := storage.SerializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow, pg := storage.BtreeGet(serKey, pg1)
	if pg == storage.DbNullPage {
		return "", errors.New("did not find table in master table")
	}

	line := storage.DeserializeRow(serRow)
	tokens := strings.Split(line, " ")
	primaryKeyType := tokens[3]
	serKey = storage.SerializeRow([]string{primaryKeyType}, []string{primaryKey})

	num, _ := strconv.Atoi(tokens[1])
	rootPgNo := uint32(num)
	rootPg, err := storage.LoadPage(rootPgNo)
	if err != nil {
		return "", err
	}

	serRow, pg = storage.BtreeGet(serKey, rootPg)
	if pg == storage.DbNullPage {
		return "", errors.New("did not find key in table")
	}

	return storage.DeserializeRow(serRow), nil
}

/*
func getTableSchema(tblName string) ([]string, []string) { // returns column names & types of the table

}
*/

func InsertIntoTable(tblName string, colTypes []string, colVals []string) error {
	pg1, err := storage.LoadPage(1)
	if err != nil {
		return err
	}

	serKey := storage.SerializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow, pg := storage.BtreeGet(serKey, pg1)
	if pg == storage.DbNullPage {
		return errors.New("did not find table in master table")
	}

	line := storage.DeserializeRow(serRow)
	num, _ := strconv.Atoi(strings.Split(line, " ")[1])
	rootPgNo := uint32(num)

	serKey = storage.SerializeRow([]string{colTypes[0]}, []string{colVals[0]})
	serRow = storage.SerializeRow(colTypes, colVals)

	firstFreePtr, err := storage.GetFirstFreePagePtr(storage.DbFilePath)
	if err != nil {
		return err
	}

	rootPg, err := storage.LoadPage(rootPgNo)
	if err != nil {
		return err
	}

	err = storage.BtreeInsert(rootPg, serKey, serRow, firstFreePtr)
	if err != nil {
		return err
	}

	return nil
}

func DeleteFromTable(tblName string, keyTypes []string, keyVals []string) error {
	pg1, err := storage.LoadPage(1)
	if err != nil {
		return err
	}

	serKey := storage.SerializeRow([]string{"VARCHAR(255)"}, []string{tblName})
	serRow, pg := storage.BtreeGet(serKey, pg1)
	if pg == storage.DbNullPage {
		return errors.New("did not find table in master table")
	}

	line := storage.DeserializeRow(serRow)
	num, _ := strconv.Atoi(strings.Split(line, " ")[1])
	rootPgNo := uint32(num)

	serKey = storage.SerializeRow(keyTypes, keyVals)

	firstFreePtr, err := storage.GetFirstFreePagePtr(storage.DbFilePath)
	if err != nil {
		return err
	}

	rootPg, err := storage.LoadPage(rootPgNo)
	if err != nil {
		return err
	}

	err = storage.BtreeDelete(rootPg, serKey, firstFreePtr)
	if err != nil {
		return err
	}

	return nil
}

func CreateTable(tblName string, colNames []string, colTypes []string) error {
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
	err = storage.SaveNewPage(tblRootPg)
	if err != nil {
		return err
	}

	pg1, err := storage.LoadPage(1)
	if err != nil {
		return err
	}

	err = storage.BtreeInsert(pg1, serKey, serRow, rootPageNo)
	if err != nil {
		return err
	}

	return nil
}
