package main

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

func main() {
	textbookTest()
}

func dispPage(pg *page) {
	fmt.Println("#############################")
	fmt.Printf("ID: %d\n", pg.id)
	if pg.pType == leafPage {
		fmt.Printf("Type: Leaf\n")
	} else {
		fmt.Printf("Type: Interior\n")
	}
	if pg.freeList != nil {
		fmt.Printf("Offset of first free block: %d\n", pg.freeList.offset)
	} else {
		fmt.Printf("Offset of first free block: NO FREE BLOCKS\n")
	}
	fmt.Printf("No. of Cells: %d\n", pg.nCells)
	fmt.Printf("Offset of cell array region: %d\n", pg.cellArrOff)
	fmt.Printf("No. of fragmented bytes: %d\n", pg.nFragBytes)

	for i := 0; i < len(pg.cellPtrArr); i++ {
		c := pg.cells[pg.cellPtrArr[i]]
		fmt.Printf("\tCell[%d]:\n", i)
		fmt.Printf("\t\tOffset: %d\n", pg.cellPtrArr[i])
		fmt.Printf("\t\tKey: %s\n", deserializeRow(c.key))
		if pg.pType == leafPage {
			fmt.Printf("\t\tRow: %s\n", deserializeRow(c.value))
		} else {
			fmt.Printf("\t\tPtr: %d\n", binary.BigEndian.Uint32(c.value))
		}
	}
	fmt.Printf("Rightmost Ptr: %d\n", pg.lastPtr)
}

func dispBtree(root *page) error { // generic BFS
	q := []uint32{}
	level := 1
	q = append(q, root.id)
	for len(q) != 0 {
		levelSz := len(q)
		fmt.Println("******************* LEVEL ", level, " *******************")
		for levelSz != 0 {
			levelSz--
			pgId := q[0]
			q = q[1:] // dequeue

			pg, err := loadPage(pgId)
			if err != nil {
				return err
			}
			dispPage(pg)
			if pg.pType == interiorPage {
				for i := 0; i < len(pg.cellPtrArr); i++ {
					q = append(q, binary.BigEndian.Uint32(pg.cells[pg.cellPtrArr[i]].value)) // enqueue children
				}
				q = append(q, pg.lastPtr)
			}
		}
		level++
	}

	return nil
}

func simpleTest() {
	err := createDB("db.rocketsql")
	if err != nil {
		fmt.Println(err)
		return
	}

	tblName := "Students"
	colNames := []string{"ID", "Name", "Gender", "Age", "Salary"}
	colTypes := []string{"INT", "VARCHAR(255)", "VARCHAR(255)", "SMALLINT", "FLOAT"}
	colVals := []string{"42", "Mohamed Hossam", "Male", "22", "1337.66"}
	colVals2 := []string{"13", "George Miller", "Male", "35", "-999.205"}

	err = createTable(tblName, colNames, colTypes)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = insertIntoTable(tblName, colTypes, colVals)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = insertIntoTable(tblName, colTypes, colVals2)
	if err != nil {
		fmt.Println(err)
		return
	}

	pg1, err := loadPage(1)
	if err != nil {
		fmt.Println(err)
		return
	}
	dispBtree(pg1)

	pg2, err := loadPage(2)
	if err != nil {
		fmt.Println(err)
		return
	}
	dispBtree(pg2)
}

func createMultipleTablesTest(n int) {
	err := createDB("db.rocketsql")
	if err != nil {
		fmt.Println(err)
		return
	}

	tblName := "Students"
	colNames := []string{"ID", "Name", "Gender", "Age", "Salary"}
	colTypes := []string{"INT", "VARCHAR(255)", "VARCHAR(255)", "SMALLINT", "FLOAT"}

	for i := 1; i <= n; i++ {
		err := createTable(tblName+strconv.Itoa(i), colNames, colTypes)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	pg1, err := loadPage(1)
	if err != nil {
		fmt.Println(err)
		return
	}
	dispBtree(pg1)

	fmt.Println("============================================================")
}

func textbookTest() { // Database System Concepts 7th Edition, P.636 Fig. 14.9
	err := createDB("db.rocketsql")
	if err != nil {
		fmt.Println(err)
		return
	}

	tblName := "Instructors"
	colNames := []string{"Name", "Dept", "Salary"}
	colTypes := []string{"VARCHAR(255)", "VARCHAR(255)", "INT"}
	allColVals := [][]string{{"Brandt", "Comp. Sci.", "92000"},
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
		{"Kim", "Elec. Eng.", "80000"},
		{"Adams", "Music", "45000"},
		{"Lamport", "History", "82000"}}

	err = createTable(tblName, colNames, colTypes)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, colVals := range allColVals {
		err = insertIntoTable(tblName, colTypes, colVals)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	p2, err := loadPage(2)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = dispBtree(p2)
	if err != nil {
		fmt.Println(err)
		return
	}
}
