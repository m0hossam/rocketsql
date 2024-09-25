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
		fmt.Printf("\tCell[%d] Offset: %d\n", i, p.cellOffsets[i])
	}
	fmt.Println(p.cells)
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
	p.nCells = 1
	p.cellOffsets = append(p.cellOffsets, dbPageSize-1)
	p.cells = append(p.cells, 1)
	p.cells = append(p.cells, 2)
	p.cells = append(p.cells, 3)
	p.nFreeBytes -= p.nCells*sizeofPageCellOffset + 3
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
