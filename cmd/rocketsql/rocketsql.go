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

	tblName := "Instructors"
	colNames := []string{"Name", "Dept", "Salary"}
	colTypes := []string{"VARCHAR(255)", "VARCHAR(255)", "INT"}

	err = api.CreateTable(tblName, colNames, colTypes)
	if err != nil {
		fmt.Println(err)
		return
	}

	pg, err := storage.LoadPage(1)
	if err != nil {
		fmt.Println(err)
		return
	}

	storage.DumpBtree(pg, "meta.txt")
}
