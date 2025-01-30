package main

import (
	"fmt"

	"github.com/m0hossam/rocketsql/internal/storage"
	"github.com/m0hossam/rocketsql/pkg/api"
)

func main() {
	fmt.Println("rocketsql> Welcome to RocketSQL...")
	err := api.CreateDB("db.rocketsql")
	if err != nil {
		fmt.Println(err)
		return
	}

	tblName := "employees"
	colNames := []string{"name", "salary", "dept"}
	colTypes := []string{"VARCHAR(255)", "INT", "VARCHAR(255)"}

	err = api.CreateTable(tblName, colNames, colTypes)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = api.InsertIntoTable(tblName, colTypes, []string{"Mohamed Hossam", "13000", "CSE"})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = api.InsertIntoTable(tblName, colTypes, []string{"Ahmed Nasr", "25000", "MPE"})
	if err != nil {
		fmt.Println(err)
		return
	}

	pg3, err := storage.LoadPage(3)
	if err != nil {
		fmt.Println(err)
		return
	}
	storage.DumpBtree(pg3, "employees.txt")
	it := storage.BtreeFirst(pg3)
	for row, isNotEnd, err := it.Next(); isNotEnd; row, isNotEnd, err = it.Next() {
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(row)
	}
}
