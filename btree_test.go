package main

import (
	"testing"
)

func TestInsertion(t *testing.T) { // Database System Concepts 7th Edition, Pages 636-642-643, Figures 14.9-14.14-14.15
	err := createDB("db.rocketsql")
	if err != nil {
		t.Fatal(err)
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
		{"Crick", "Biology", "72000"}}
	newColVals := [][]string{
		{"Kim", "Elec. Eng.", "80000"},
		{"Adams", "Music", "45000"},
		{"Lamport", "History", "82000"}}
	expectedFiles := []string{"test_data/InsertionSimpleCase.txt",
		"test_data/InsertionLeafOverflow.txt",
		"test_data/InsertionInteriorOverflow.txt"}

	err = createTable(tblName, colNames, colTypes)
	if err != nil {
		t.Fatal(err)
	}

	for _, colVals := range oldColVals {
		err = insertIntoTable(tblName, colTypes, colVals)
		if err != nil {
			t.Fatal(err)
		}
	}

	for idx, colVals := range newColVals {
		err = insertIntoTable(tblName, colTypes, colVals)
		if err != nil {
			t.Fatal(err)
		}

		rootPg, err := loadPage(3)
		if err != nil {
			t.Fatal(err)
		}

		expected := expectedFiles[idx]
		actual := "output.txt"

		err = dumpBtree(rootPg, actual)
		if err != nil {
			t.Fatal(err)
		}

		equal, err := compareFilesLineByLine(expected, actual)
		if err != nil {
			t.Fatal(err)
		}

		if !equal {
			t.Fatalf("Insertion failed: actual output is different from expected output at %s", expected)
		}
	}
}
