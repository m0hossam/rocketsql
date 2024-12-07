package main

import (
	"fmt"
)

func main() {
	fmt.Println("rocketsql> Welcome to RocketSQL...")
	err := testDeletion()
	if err != nil {
		fmt.Print(err)
		return
	}
}

func testDeletion() error {
	err := createDB("db.rocketsql")
	if err != nil {
		return err
	}

	dbMaxCellsPerPage = 3 // Essential for this test case

	tblName := "Instructors"
	colNames := []string{"Name", "Dept", "Salary"}
	colTypes := []string{"VARCHAR(255)", "VARCHAR(255)", "INT"}
	oldColVals := [][]string{{"Brandt", "Comp. Sci.", "92000"},
		{"Califieri", "History", "60000"},
		{"Einstein", "Physics", "95000"},
		{"El Said", "History", "80000"},
		{"Gold", "Physics", "87000"},
		{"Katz", "Comp. Sci.", "75000"},
		{"Mozart", "Music", "40000"},
		{"Singh", "Finance", "80000"},
		{"Srinivasan", "Comp. Sci.", "65000"},
		{"Wu", "Finance", "90000"},
		{"Crick", "Biology", "72000"},
		{"Kim", "Comp. Sci.", "75000"}}

	err = createTable(tblName, colNames, colTypes)
	if err != nil {
		return err
	}

	for _, colVals := range oldColVals {
		err = insertIntoTable(tblName, colTypes, colVals)
		if err != nil {
			return err
		}
	}

	rootPg, err := loadPage(3)
	if err != nil {
		return err
	}
	err = dumpBtree(rootPg, "initial.txt")
	if err != nil {
		return err
	}

	for _, colVals := range oldColVals {
		err = deleteFromTable(tblName, []string{colTypes[0]}, []string{colVals[0]})
		if err != nil {
			return err
		}
	}

	rootPg, err = loadPage(3)
	if err != nil {
		return err
	}
	err = dumpBtree(rootPg, "middle.txt")
	if err != nil {
		return err
	}

	for _, colVals := range oldColVals {
		err = insertIntoTable(tblName, colTypes, colVals)
		if err != nil {
			return err
		}
	}

	rootPg, err = loadPage(3)
	if err != nil {
		return err
	}
	err = dumpBtree(rootPg, "final.txt")
	if err != nil {
		return err
	}

	eq, err := compareFilesLineByLine("initial.txt", "final.txt")
	if err != nil {
		return err
	}

	if eq {
		fmt.Println("IDENTICAL")
	} else {
		fmt.Println("NOT IDENTICAL")
	}

	return nil
}
