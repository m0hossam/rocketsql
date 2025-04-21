package main

import (
	"fmt"

	"github.com/m0hossam/rocketsql/db"
	"github.com/m0hossam/rocketsql/record"
)

func main() {
	fmt.Println("rocketsql> Welcome to RocketSQL")
	runDbExample()
}

func printTable(db *db.Db, tblName string) {
	// print table schema
	fmt.Println("---------------------------------")
	fmt.Println("Table [" + tblName + "]")
	fmt.Println("---------------------------------")
	rootPgNo, names, _, err := db.GetTableMetaData(tblName)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i, name := range names {
		s := name
		if i != len(names)-1 {
			s += "|"
		}
		fmt.Print(s)
	}
	fmt.Println("\n---------------------------------")

	// print table rows
	it, err := db.GetBtreeIterator(rootPgNo)
	if err != nil {
		fmt.Println(err)
		return
	}

	for row, isNotEnd, err := it.Next(); isNotEnd; row, isNotEnd, err = it.Next() {
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(record.DeserializeRow(row))
	}
	fmt.Println("---------------------------------")
	fmt.Println()
}

func runDbExample() {
	fmt.Println("rocketsql> Welcome to RocketSQL...")
	db, err := db.NewDb("rocketsql.db")
	if err != nil {
		fmt.Println(err)
		return
	}

	tblName := "employees"
	colNames := []string{"name", "salary", "dept"} // 1st column is the PK by default
	colTypes := []string{"VARCHAR(32)", "INT", "VARCHAR(16)"}

	if err := db.CreateTable(tblName, colNames, colTypes); err != nil {
		fmt.Println(err)
		return
	}

	rows := [][]string{
		{"Mohamed Hossam", "13000", "CSE"},
		{"Ahmed Nasr", "25000", "MPE"},
		{"Moataz Mokhtar", "30000", "ECE"},
		{"Salma El-Sayed", "22500", "ARC"},
		{"Mina Fayed", "33500", "CHE"},
	}

	// insert all rows
	for _, row := range rows {
		if err := db.InsertRow(tblName, row); err != nil {
			fmt.Println(err)
			return
		}
	}
	printTable(db, tblName)

	// delete one row
	if err := db.DeleteRow(tblName, "Mohamed Hossam"); err != nil {
		fmt.Println(err)
		return
	}
	printTable(db, tblName)

	// update one row
	if err := db.UpdateRow(tblName, "Mina Fayed", []string{"Mina Fayed", "77777", "PRO"}); err != nil {
		fmt.Println(err)
		return
	}
	printTable(db, tblName)

	fmt.Println("rocketsql> Bye!")
}
