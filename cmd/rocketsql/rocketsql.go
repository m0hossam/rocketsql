package main

import (
	"fmt"

	"github.com/m0hossam/rocketsql/internal/db"
	"github.com/m0hossam/rocketsql/internal/storage"
)

func main() {
	examineMetaTableExample()
	runDbExample()
}

func examineMetaTableExample() {
	fmt.Println("rocketsql> Welcome to RocketSQL...")
	db, err := db.CreateDb("db")
	if err != nil {
		fmt.Println(err)
		return
	}
	tblName := "Instructors"
	colNames := []string{"Name", "Dept", "Salary"}
	colTypes := []string{"VARCHAR(255)", "VARCHAR(255)", "INT"}
	err = db.CreateTable(tblName, colNames, colTypes)
	if err != nil {
		fmt.Println(err)
		return
	}
	pg, err := db.Pgr.LoadPage(1)
	if err != nil {
		fmt.Println(err)
		return
	}
	storage.DumpBtree(db.Btree, pg, "meta.txt")
	fmt.Println("rocketsql> Bye!")
}

func runDbExample() {
	fmt.Println("rocketsql> Welcome to RocketSQL...")
	db, err := db.CreateDb("db")
	if err != nil {
		fmt.Println(err)
		return
	}

	tblName := "employees"
	colNames := []string{"name", "salary", "dept"}
	colTypes := []string{"VARCHAR(255)", "INT", "VARCHAR(255)"}

	err = db.CreateTable(tblName, colNames, colTypes)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = db.InsertIntoTable(tblName, []string{"Mohamed Hossam", "13000", "CSE"})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = db.InsertIntoTable(tblName, []string{"Ahmed Nasr", "25000", "MPE"})
	if err != nil {
		fmt.Println(err)
		return
	}

	pg3, err := db.Pgr.LoadPage(3)
	if err != nil {
		fmt.Println(err)
		return
	}
	storage.DumpBtree(db.Btree, pg3, "employees.txt")
	it := db.Btree.BtreeFirst(pg3)
	for row, isNotEnd, err := it.Next(); isNotEnd; row, isNotEnd, err = it.Next() {
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(row)
	}
	fmt.Println("rocketsql> Bye!")
}
