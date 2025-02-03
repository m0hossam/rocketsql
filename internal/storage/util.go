package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

func CompareFilesLineByLine(file1 string, file2 string) (bool, error) {
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

	// If we reached here, the files are identical
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

func dumpPage(pg *page, file *os.File) {
	writeToFile(file, "#############################\n")
	writeToFile(file, "ID: %d\n", pg.id)
	if pg.pType == LeafPage {
		writeToFile(file, "Type: Leaf\n")
	} else {
		writeToFile(file, "Type: Interior\n")
	}
	if pg.freeList != nil {
		writeToFile(file, "Offset of first free block: %d\n", pg.freeList.offset)
	} else {
		writeToFile(file, "Offset of first free block: NO FREE BLOCKS\n")
	}
	writeToFile(file, "No. of Cells: %d\n", pg.nCells)
	writeToFile(file, "Offset of cell array region: %d\n", pg.cellArrOff)
	writeToFile(file, "No. of fragmented bytes: %d\n", pg.nFragBytes)

	for i := 0; i < len(pg.cellPtrArr); i++ {
		c := pg.cells[pg.cellPtrArr[i]]
		writeToFile(file, "\tCell[%d]:\n", i)
		writeToFile(file, "\t\tOffset: %d\n", pg.cellPtrArr[i])
		writeToFile(file, "\t\tKey: %s\n", DeserializeRow(c.key))
		if pg.pType == LeafPage {
			writeToFile(file, "\t\tRow: %s\n", DeserializeRow(c.value))
		} else {
			writeToFile(file, "\t\tPtr: %d\n", binary.BigEndian.Uint32(c.value))
		}
	}
	writeToFile(file, "Rightmost Ptr: %d\n", pg.lastPtr)
}

func DumpBtree(btree *Btree, root *page, filePath string) error { // generic BFS
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()

	q := []uint32{}
	level := 1
	q = append(q, root.id)
	for len(q) != 0 {
		levelSz := len(q)
		writeToFile(file, "******************* LEVEL %d *******************\n", level)
		for levelSz != 0 {
			levelSz--
			pgId := q[0]
			q = q[1:] // dequeue

			pg, err := btree.pgr.LoadPage(pgId)
			if err != nil {
				return err
			}
			dumpPage(pg, file)
			if pg.pType == InteriorPage {
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
