package main

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

func main() {
	err := openDB(dbFilePath)
	if err != nil {
		err = createDB("db.rocketsql")
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	createMultipleTablesTest(100)
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
	tblName := "Students"
	colNames := []string{"ID", "Name", "Gender", "Age", "Salary"}
	colTypes := []string{"INT", "VARCHAR(255)", "VARCHAR(255)", "SMALLINT", "FLOAT"}
	colVals := []string{"42", "Mohamed Hossam", "Male", "22", "1337.66"}
	colVals2 := []string{"13", "George Miller", "Male", "35", "-999.205"}

	err := createTable(tblName, colNames, colTypes)
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

	/*
		p, err := getFirstFreePagePtr(dbFilePath)
		if err != nil {
			fmt.Println(err)
			return
		}
		for i := uint32(1); i <= *(p)-1; i++ {
			pg, err := loadPage(i)
			if err != nil {
				fmt.Println(err)
				return
			}
			dispPage(pg)
		}
	*/
}
