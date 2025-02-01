package api

import (
	"testing"

	"github.com/m0hossam/rocketsql/internal/storage"
)

const (
	srcDirPath = "../../"
)

func TestInsertion(t *testing.T) { // Database System Concepts 7th Edition, Pages 636-642-643, Figures 14.9-14.14-14.15
	err := CreateDB(srcDirPath + "./db.rocketsql")
	if err != nil {
		t.Fatal(err)
	}

	storage.DbMaxCellsPerPage = 3 // Essential for this test case

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

	err = CreateTable(tblName, colNames, colTypes)
	if err != nil {
		t.Fatal(err)
	}

	for _, colVals := range oldColVals {
		err = InsertIntoTable(tblName, colVals)
		if err != nil {
			t.Fatal(err)
		}
	}

	for idx, colVals := range newColVals {
		err = InsertIntoTable(tblName, colVals)
		if err != nil {
			t.Fatal(err)
		}

		rootPg, err := storage.LoadPage(3)
		if err != nil {
			t.Fatal(err)
		}

		expected := srcDirPath + expectedFiles[idx]
		actual := srcDirPath + "temp/output.txt"

		err = storage.DumpBtree(rootPg, actual)
		if err != nil {
			t.Fatal(err)
		}

		equal, err := storage.CompareFilesLineByLine(expected, actual)
		if err != nil {
			t.Fatal(err)
		}

		if !equal {
			t.Fatalf("Deletion failed: actual output (%s) is different from expected output (%s)", actual, expected)
		}
	}
}

func TestDeletion(t *testing.T) {
	err := CreateDB(srcDirPath + "./db.rocketsql")
	if err != nil {
		t.Fatal(err)
	}

	storage.DbMaxCellsPerPage = 3 // Essential for this test case

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

	err = CreateTable(tblName, colNames, colTypes)
	if err != nil {
		t.Fatal(err)
	}

	for _, colVals := range oldColVals {
		err = InsertIntoTable(tblName, colVals)
		if err != nil {
			t.Fatal(err)
		}
	}

	expected := srcDirPath + "temp/initial.txt"
	actual := srcDirPath + "temp/final.txt"

	rootPg, err := storage.LoadPage(3)
	if err != nil {
		t.Fatal(err)
	}
	err = storage.DumpBtree(rootPg, expected)
	if err != nil {
		t.Fatal(err)
	}

	for _, colVals := range oldColVals {
		err = DeleteFromTable(tblName, colVals[0])
		if err != nil {
			t.Fatal(err)
		}
	}

	rootPg, err = storage.LoadPage(3)
	if err != nil {
		t.Fatal(err)
	}
	err = storage.DumpBtree(rootPg, srcDirPath+"temp/middle.txt")
	if err != nil {
		t.Fatal(err)
	}

	for _, colVals := range oldColVals {
		err = InsertIntoTable(tblName, colVals)
		if err != nil {
			t.Fatal(err)
		}
	}

	rootPg, err = storage.LoadPage(3)
	if err != nil {
		t.Fatal(err)
	}
	err = storage.DumpBtree(rootPg, actual)
	if err != nil {
		t.Fatal(err)
	}

	eq, err := storage.CompareFilesLineByLine(expected, actual)
	if err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatalf("Deletion failed: actual output (%s) is different from expected output (%s)", actual, expected)
	}
}
