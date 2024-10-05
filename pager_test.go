package main

import (
	"fmt"
	"testing"
)

func displayPageOne(p *pageOne) {
	fmt.Printf("ID: %d\n", 1)
	fmt.Printf("DB Page Size: %d\n", p.pSize)
	fmt.Printf("DB First Free Page Ptr: %d\n", p.firstFreePtr)
	if p.pType == leafPage {
		fmt.Printf("Type: Leaf\n")
	} else {
		fmt.Printf("Type: Interior\n")
	}
	fmt.Printf("No. of Free Bytes: %d\n", p.nFreeBytes)
	fmt.Printf("Rightmost Ptr: %d\n", p.lastPtr)
	fmt.Printf("No. of Cells: %d\n", p.nCells)
	for i := 0; i < int(p.nCells); i++ {
		fmt.Printf("\tCell[%d] Offset: %d\n", i, p.cellOffsets[i])
	}
	fmt.Println(p.cells)
}

func displayPage(p *page) {
	fmt.Println("#############################")
	fmt.Printf("ID: %d\n", p.id)
	if p.pType == leafPage {
		fmt.Printf("Type: Leaf\n")
	} else {
		fmt.Printf("Type: Interior\n")
	}
	fmt.Printf("No. of Free Bytes: %d\n", p.nFreeBytes)
	fmt.Printf("Rightmost Ptr: %d\n", p.lastPtr)
	fmt.Printf("No. of Cells: %d\n", p.nCells)
	for i := 0; i < int(p.nCells); i++ {
		fmt.Printf("\tCell[%d]:\n", i)
		fmt.Printf("\t\tOffset: %d\n", p.cellOffArr[i])
		fmt.Printf("\t\tKey: %d\n", p.cells[p.cellOffArr[i]].key)
		if p.pType == leafPage {
			fmt.Printf("\t\tPayload Size: %d\n", p.cells[p.cellOffArr[i]].payloadSize)
			fmt.Printf("\t\tPayload: %v\n", p.cells[p.cellOffArr[i]].payload)
		} else {
			fmt.Printf("\t\tPointer: %d\n", p.cells[p.cellOffArr[i]].ptr)
		}
	}
}

func TestCreateDB(t *testing.T) {
	path := "db.rocketsql"
	err := createDB(path)
	if err != nil {
		t.Fatalf("Failed to create DB '%s': %s", path, err.Error())
	}
}

func TestOpenDB(t *testing.T) {
	path := "db.rocketsql"
	err := openDB(path)
	if err != nil {
		t.Fatalf("Failed to open DB '%s': %s", path, err.Error())
	}
}

func TestLoadPageOne(t *testing.T) {
	p, err := loadPageOne()
	if err != nil {
		t.Fatalf("failed to load page: %s", err.Error())
	}
	displayPageOne(p)
}

func TestSaveNewPage(t *testing.T) {
	var freePgPtr uint32 = 2
	p, err := createPage(leafPage, &freePgPtr)
	if err != nil {
		t.Fatalf("failed to create a new page: %s", err.Error())
	}
	err = saveNewPage(p)
	if err != nil {
		t.Fatalf("failed to save new page: %s", err.Error())
	}
}

func TestLoadPage(t *testing.T) {
	p, err := loadPage(2)
	if err != nil {
		t.Fatalf("failed to load page: %s", err.Error())
	}
	displayPage(p)
}

func TestSavePage(t *testing.T) {
	p, err := loadPage(2)
	if err != nil {
		t.Fatalf("failed to load page: %s", err.Error())
	}
	fmt.Println("Loaded page from disk: ###########")
	displayPage(p)

	p.lastPtr = 99
	p.nCells++
	p.cellOffArr = append(p.cellOffArr, 4000)
	c := cell{
		key:         74,
		payloadSize: 2,
		payload:     []byte{66, 99},
	}
	p.cells[4000] = c
	p.nFreeBytes -= 8 + 2
	fmt.Println("Loaded page in-memory after updating: ###########")
	displayPage(p)

	err = savePage(p)
	if err != nil {
		t.Fatalf("failed to save page: %s", err.Error())
	}

	p, err = loadPage(2)
	if err != nil {
		t.Fatalf("failed to load page: %s", err.Error())
	}
	fmt.Println("Loaded page from disk after updating: ###########")
	displayPage(p)
}

func TestBtreeExample(t *testing.T) {
	err := openDB("db.rocketsql")
	if err != nil {
		msg := createAndSeedDB()
		if msg != "ok" {
			t.Fatal(msg)
		}
	}

	for i := 2; i <= 9; i++ {
		p, err := loadPage(uint32(i))
		if err != nil {
			t.Fatalf("Failed to load page: %s", err)
		}
		displayPage(p)
	}
}
