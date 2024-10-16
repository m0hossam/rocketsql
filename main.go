package main

import (
	"encoding/binary"
	"fmt"
	"sort"
)

var (
	keyNameMap = map[int]string{}
	nameKeyMap = map[string]int{}
)

func main() { // MAIN WILL BE USED FOR DEBUGGING BECAUSE I CANT DEBUG TESTS FOR SOME REASON
	fmt.Println("Hello, world")
	testWithNames()
}

/*
Test this with dbPageSize = 50
REFERENCE: Database System Concepts 7th Edition P. 636 Figure 14.9
*/
func testWithNames() {
	nameMaxLength := 5
	names := []string{"Brand", "Calif", "Einst", "El Sa", "Gold", "Katz", "Mozar", "Singh", "Srini", "Wu", "Crick", "Kim"}
	for idx, name := range names { // precaution for future updates to array
		end := min(len(name), nameMaxLength)
		buf := []byte(name[:end])
		names[idx] = string(buf) // shorten names to a max length of 5
	}

	sortedNames := make([]string, len(names))
	copy(sortedNames, names)
	sort.Strings(sortedNames)
	for idx, name := range sortedNames {
		keyNameMap[idx] = name
		nameKeyMap[name] = idx
	}

	err := createDB("db.rocketsql")
	if err != nil {
		fmt.Println(err)
		return
	}

	p1, err := loadPageOne()
	if err != nil {
		fmt.Println(err)
		return
	}

	p2, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = saveNewPage(p2)
	if err != nil {
		fmt.Println(err)
		return
	}

	rootId := uint32(2)
	for _, name := range names {
		root, err := loadPage(rootId)
		if err != nil {
			fmt.Println(err)
			return
		}

		buf := make([]byte, nameMaxLength)
		copy(buf, name)

		newRootId, err := insert(uint32(nameKeyMap[name]), buf, root, &p1.firstFreePtr)
		if err != nil {
			fmt.Println(err)
			return
		}
		if newRootId != dbNullPage {
			rootId = newRootId
		}
	}

	root, err := loadPage(rootId)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = dispBtree(root)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func testWithNumbers() {
	for i := 0; i < 15; i++ {
		err := testInsert(30 + i*5)
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
				for i := 0; i < len(pg.cellOffArr); i++ {
					q = append(q, pg.cells[pg.cellOffArr[i]].ptr) // enqueue children
				}
				q = append(q, pg.lastPtr)
			}
		}
		level++
	}

	return nil
}

func dispPage(pg *page) {
	fmt.Println("#############################")
	fmt.Printf("ID: %d\n", pg.id)
	if pg.pType == leafPage {
		fmt.Printf("Type: Leaf\n")
	} else {
		fmt.Printf("Type: Interior\n")
	}
	fmt.Printf("No. of Free Bytes: %d\n", pg.nFreeBytes)
	fmt.Printf("No. of Cells: %d\n", pg.nCells)
	for i := 0; i < len(pg.cellOffArr); i++ {
		c := pg.cells[pg.cellOffArr[i]]
		fmt.Printf("\tCell[%d]:\n", i)
		fmt.Printf("\t\tOffset: %d\n", pg.cellOffArr[i])
		fmt.Printf("\t\tKey: %v\n", keyNameMap[int(c.key)]) // TODO: ADJUST THIS BASED ON KEY DATATYPE
		if pg.pType == leafPage {
			fmt.Printf("\t\tPayload Size: %d\n", c.payloadSize)
			fmt.Printf("\t\tPayload: %v\n", string(c.payload)) // TODO: ADJUST THIS BASED ON PAYLOAD DATATYPE
		} else {
			fmt.Printf("\t\tPointer: %d\n", c.ptr)
		}
	}
	fmt.Printf("Rightmost Ptr: %d\n", pg.lastPtr)
}

func testInsert(val int) error {
	err := openDB("db.rocketsql")
	if err != nil {
		err = createAndSeedDB()
		if err != nil {
			return err
		}
	}

	p1, err := loadPageOne()
	if err != nil {
		return fmt.Errorf("failed to load page one: %s", err)
	}

	root, err := loadPage(2)
	if err != nil {
		return fmt.Errorf("failed to load page: %s", err)
	}

	// INSERTION PART ##################
	key := uint32(val)
	payload := make([]byte, 2)
	binary.Encode(payload, binary.BigEndian, uint16(val))
	newRootId, err := insert(key, payload, root, &p1.firstFreePtr)
	if err != nil {
		return fmt.Errorf("failed to insert cell: %s", err)
	}
	if newRootId != dbNullPage {
		fmt.Println("NEW ROOT CREATED WITH AN ID OF ", newRootId)
	}
	// ###################################

	err = savePageOne(p1)
	if err != nil {
		return fmt.Errorf("failed to save page one to disk: %s", err)
	}

	return nil
}
