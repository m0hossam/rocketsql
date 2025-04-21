package db

/*
IMPORTANT: For this test suite to succeed, the MaxCellsPerPage constant must be set to 3.
TODO: REMOVE THIS FILE
*/

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"testing"

	"github.com/m0hossam/rocketsql/btree"
	"github.com/m0hossam/rocketsql/page"
	"github.com/m0hossam/rocketsql/record"
)

const (
	srcDirPath = "../"
)

func TestInsertion(t *testing.T) { // Database System Concepts 7th Edition, Pages 636-642-643, Figures 14.9-14.14-14.15
	db, err := NewDb(srcDirPath + "rocketsql.db")
	if err != nil {
		t.Fatal(err)
	}

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

	err = db.CreateTable(tblName, colNames, colTypes)
	if err != nil {
		t.Fatal(err)
	}

	for _, colVals := range oldColVals {
		err = db.InsertRow(tblName, colVals)
		if err != nil {
			t.Fatal(err)
		}
	}

	for idx, colVals := range newColVals {
		err = db.InsertRow(tblName, colVals)
		if err != nil {
			t.Fatal(err)
		}

		rootPg, err := db.btree.GetPager().ReadPage(3)
		if err != nil {
			t.Fatal(err)
		}

		expected := srcDirPath + expectedFiles[idx]
		actual := srcDirPath + "temp/output.txt"

		err = dumpBtree(db.btree, rootPg, actual)
		if err != nil {
			t.Fatal(err)
		}

		equal, err := compareFilesLineByLine(expected, actual)
		if err != nil {
			t.Fatal(err)
		}

		if !equal {
			t.Fatalf("Insertion failed: actual output (%s) is different from expected output (%s)", actual, expected)
		}
	}
}

func TestDeletion(t *testing.T) {
	db, err := NewDb(srcDirPath + "rocketsql.db")
	if err != nil {
		t.Fatal(err)
	}

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

	err = db.CreateTable(tblName, colNames, colTypes)
	if err != nil {
		t.Fatal(err)
	}

	for _, colVals := range oldColVals {
		err = db.InsertRow(tblName, colVals)
		if err != nil {
			t.Fatal(err)
		}
	}

	expected := srcDirPath + "temp/initial.txt"
	actual := srcDirPath + "temp/final.txt"

	rootPg, err := db.btree.GetPager().ReadPage(3)
	if err != nil {
		t.Fatal(err)
	}
	err = dumpBtree(db.btree, rootPg, expected)
	if err != nil {
		t.Fatal(err)
	}

	for _, colVals := range oldColVals {
		err = db.DeleteRow(tblName, colVals[0])
		if err != nil {
			t.Fatal(err)
		}
	}

	rootPg, err = db.btree.GetPager().ReadPage(3)
	if err != nil {
		t.Fatal(err)
	}
	err = dumpBtree(db.btree, rootPg, srcDirPath+"temp/middle.txt")
	if err != nil {
		t.Fatal(err)
	}

	for _, colVals := range oldColVals {
		err = db.InsertRow(tblName, colVals)
		if err != nil {
			t.Fatal(err)
		}
	}

	rootPg, err = db.btree.GetPager().ReadPage(3)
	if err != nil {
		t.Fatal(err)
	}
	err = dumpBtree(db.btree, rootPg, actual)
	if err != nil {
		t.Fatal(err)
	}

	eq, err := compareFilesLineByLine(expected, actual)
	if err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatalf("Deletion failed: actual output (%s) is different from expected output (%s)", actual, expected)
	}
}

func compareFilesLineByLine(file1 string, file2 string) (bool, error) {
	// Open the first file
	f1, err := os.Open(file1)
	if err != nil {
		return false, fmt.Errorf("could not open file %s: %v", file1, err)
	}
	defer f1.Close()

	// Open the second file
	f2, err := os.Open(file2)
	if err != nil {
		return false, fmt.Errorf("could not open file %s: %v", file2, err)
	}
	defer f2.Close()

	// Create scanners to read each file line by line
	scanner1 := bufio.NewScanner(f1)
	scanner2 := bufio.NewScanner(f2)

	// Compare each line
	for scanner1.Scan() && scanner2.Scan() {
		if scanner1.Text() != scanner2.Text() {
			return false, nil // Files differ at this line
		}
	}

	// Check if both files have the same number of lines
	if scanner1.Scan() || scanner2.Scan() {
		return false, nil // One file has more lines than the other
	}

	// If we reached here, the files are Identical
	return true, nil
}

func writeToFile(file *os.File, format string, args ...interface{}) {
	// Create a new writer and write to the file
	writer := bufio.NewWriter(file)
	_, err := fmt.Fprintf(writer, format, args...)
	if err != nil {
		fmt.Println("Error writing to file:", err)
	}
	writer.Flush() // Ensure all buffered data is written to the file
}

func dumpPage(pg *page.Page, file *os.File) {
	writeToFile(file, "#############################\n")
	writeToFile(file, "ID: %d\n", pg.Id)
	if pg.Type == page.LeafPage {
		writeToFile(file, "Type: Leaf\n")
	} else {
		writeToFile(file, "Type: Interior\n")
	}
	if pg.FreeList != nil {
		writeToFile(file, "Offset of first free block: %d\n", pg.FreeList.Offset)
	} else {
		writeToFile(file, "Offset of first free block: NO FREE BLOCKS\n")
	}
	writeToFile(file, "No. of Cells: %d\n", pg.NumCells)
	writeToFile(file, "Offset of cell array region: %d\n", pg.CellArrOff)
	writeToFile(file, "No. of fragmented bytes: %d\n", pg.NumFragBytes)

	for i := 0; i < len(pg.CellPtrArr); i++ {
		c := pg.Cells[pg.CellPtrArr[i]]
		writeToFile(file, "\tCell[%d]:\n", i)
		writeToFile(file, "\t\tOffset: %d\n", pg.CellPtrArr[i])
		writeToFile(file, "\t\tKey: %s\n", record.DeserializeRow(c.Key))
		if pg.Type == page.LeafPage {
			writeToFile(file, "\t\tRow: %s\n", record.DeserializeRow(c.Value))
		} else {
			writeToFile(file, "\t\tPtr: %d\n", binary.BigEndian.Uint32(c.Value))
		}
	}
	writeToFile(file, "Rightmost Ptr: %d\n", pg.LastPtr)
}

func dumpBtree(btree *btree.Btree, root *page.Page, filePath string) error { // generic BFS
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()

	q := []uint32{}
	level := 1
	q = append(q, root.Id)
	for len(q) != 0 {
		levelSz := len(q)
		writeToFile(file, "******************* LEVEL %d *******************\n", level)
		for levelSz != 0 {
			levelSz--
			pgId := q[0]
			q = q[1:] // dequeue

			pg, err := btree.GetPager().ReadPage(pgId)
			if err != nil {
				return err
			}
			dumpPage(pg, file)
			if pg.Type == page.InteriorPage {
				for i := 0; i < len(pg.CellPtrArr); i++ {
					q = append(q, binary.BigEndian.Uint32(pg.Cells[pg.CellPtrArr[i]].Value)) // enqueue children
				}
				q = append(q, pg.LastPtr)
			}
		}
		level++
	}

	return nil
}
