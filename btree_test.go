package main

import (
	"fmt"
	"testing"
)

func createAndSeedDB() string {
	err := createDB("db.rocketsql")
	if err != nil {
		return fmt.Sprintf("Failed to create DB: %s", err)
	}

	p1, err := loadPageOne()
	if err != nil {
		return fmt.Sprintf("Failed to open page one: %s", err)
	}

	p2, err := createPage(interiorPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Sprintf("Failed to open page: %s", err)
	}
	p2.nCells = 1
	p2.lastPtr = 4
	p2.cellOffsets = append(p2.cellOffsets, dbPageSize-sizeofPageCellKey-sizeofPageCellPtr)
	p2.cells = append(p2.cells, 0, 0, 0, 13, 0, 0, 0, 3)
	p2.nFreeBytes = dbPageSize - dbPageHdrSize - p2.nCells*sizeofPageCellOffset - p2.nCells*8
	err = saveNewPage(p2)
	if err != nil {
		return fmt.Sprintf("Failed to save page to disk: %s", err)
	}

	p3, err := createPage(interiorPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Sprintf("Failed to open page: %s", err)
	}
	p3.nCells = 2
	p3.lastPtr = 7
	p3.cellOffsets = append(p3.cellOffsets, dbPageSize-sizeofPageCellKey-sizeofPageCellPtr)
	p3.cellOffsets = append(p3.cellOffsets, dbPageSize-2*sizeofPageCellKey-2*sizeofPageCellPtr)
	p3.cells = append(p3.cells, 0, 0, 0, 11, 0, 0, 0, 6, 0, 0, 0, 9, 0, 0, 0, 5)
	p3.nFreeBytes = dbPageSize - dbPageHdrSize - p3.nCells*sizeofPageCellOffset - p3.nCells*8
	err = saveNewPage(p3)
	if err != nil {
		return fmt.Sprintf("Failed to save page to disk: %s", err)
	}

	p4, err := createPage(interiorPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Sprintf("Failed to open page: %s", err)
	}
	p4.nCells = 1
	p4.lastPtr = 9
	p4.cellOffsets = append(p4.cellOffsets, dbPageSize-sizeofPageCellKey-sizeofPageCellPtr)
	p4.cells = append(p4.cells, 0, 0, 0, 16, 0, 0, 0, 8)
	p4.nFreeBytes = dbPageSize - dbPageHdrSize - p4.nCells*sizeofPageCellOffset - p4.nCells*8
	err = saveNewPage(p4)
	if err != nil {
		return fmt.Sprintf("Failed to save page to disk: %s", err)
	}

	p5, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Sprintf("Failed to open page: %s", err)
	}
	p5.nCells = 2
	p5.lastPtr = 6
	p5.cellOffsets = append(p5.cellOffsets, dbPageSize-sizeofPageCellKey-sizeofPageCellPayloadSize-1, dbPageSize-2*sizeofPageCellKey-2*sizeofPageCellPayloadSize-2)
	p5.cells = append(p5.cells, 0, 0, 0, 4, 0, 1, 4, 0, 0, 0, 1, 0, 1, 1)
	p5.nFreeBytes = dbPageSize - dbPageHdrSize - p5.nCells*sizeofPageCellOffset - p5.nCells*7
	err = saveNewPage(p5)
	if err != nil {
		return fmt.Sprintf("Failed to save page to disk: %s", err)
	}

	p6, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Sprintf("Failed to open page: %s", err)
	}
	p6.nCells = 2
	p6.lastPtr = 7
	p6.cellOffsets = append(p6.cellOffsets, dbPageSize-sizeofPageCellKey-sizeofPageCellPayloadSize-1, dbPageSize-2*sizeofPageCellKey-2*sizeofPageCellPayloadSize-2)
	p6.cells = append(p6.cells, 0, 0, 0, 10, 0, 1, 10, 0, 0, 0, 9, 0, 1, 9)
	p6.nFreeBytes = dbPageSize - dbPageHdrSize - p6.nCells*sizeofPageCellOffset - p6.nCells*7
	err = saveNewPage(p6)
	if err != nil {
		return fmt.Sprintf("Failed to save page to disk: %s", err)
	}

	p7, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Sprintf("Failed to open page: %s", err)
	}
	p7.nCells = 2
	p7.lastPtr = 8
	p7.cellOffsets = append(p7.cellOffsets, dbPageSize-sizeofPageCellKey-sizeofPageCellPayloadSize-1, dbPageSize-2*sizeofPageCellKey-2*sizeofPageCellPayloadSize-2)
	p7.cells = append(p7.cells, 0, 0, 0, 12, 0, 1, 12, 0, 0, 0, 11, 0, 1, 11)
	p7.nFreeBytes = dbPageSize - dbPageHdrSize - p7.nCells*sizeofPageCellOffset - p7.nCells*7
	err = saveNewPage(p7)
	if err != nil {
		return fmt.Sprintf("Failed to save page to disk: %s", err)
	}

	p8, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Sprintf("Failed to open page: %s", err)
	}
	p8.nCells = 2
	p8.lastPtr = 9
	p8.cellOffsets = append(p8.cellOffsets, dbPageSize-sizeofPageCellKey-sizeofPageCellPayloadSize-1, dbPageSize-2*sizeofPageCellKey-2*sizeofPageCellPayloadSize-2)
	p8.cells = append(p8.cells, 0, 0, 0, 15, 0, 1, 15, 0, 0, 0, 13, 0, 1, 13)
	p8.nFreeBytes = dbPageSize - dbPageHdrSize - p8.nCells*sizeofPageCellOffset - p8.nCells*7
	err = saveNewPage(p8)
	if err != nil {
		return fmt.Sprintf("Failed to save page to disk: %s", err)
	}

	p9, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Sprintf("Failed to open page: %s", err)
	}
	p9.nCells = 3
	p9.lastPtr = dbNullPage
	p9.cellOffsets = append(p9.cellOffsets, dbPageSize-sizeofPageCellKey-sizeofPageCellPayloadSize-1, dbPageSize-2*sizeofPageCellKey-2*sizeofPageCellPayloadSize-2, dbPageSize-3*sizeofPageCellKey-3*sizeofPageCellPayloadSize-3)
	p9.cells = append(p9.cells, 0, 0, 0, 25, 0, 1, 25, 0, 0, 0, 20, 0, 1, 20, 0, 0, 0, 16, 0, 1, 16)
	p9.nFreeBytes = dbPageSize - dbPageHdrSize - p9.nCells*sizeofPageCellOffset - p9.nCells*7
	err = saveNewPage(p9)
	if err != nil {
		return fmt.Sprintf("Failed to save page to disk: %s", err)
	}

	err = savePageOne(p1)
	if err != nil {
		return fmt.Sprintf("Failed to save page one to disk: %s", err)
	}

	return "ok"
}

func TestFindPage(t *testing.T) {
	err := openDB("db.rocketsql")
	if err != nil {
		msg := createAndSeedDB()
		if msg != "ok" {
			t.Fatal(msg)
		}
	}

	root, err := loadPage(2)
	if err != nil {
		t.Fatalf("Failed to load page: %s", err)
	}
	for i := 0; i <= 30; i++ {
		path := findPage(uint32(i), root)
		pgNum := path[len(path)-1]
		if pgNum != dbNullPage {
			fmt.Printf("Key %d should be at Page %d\n", i, pgNum)
		} else {
			fmt.Printf("Key %d NOT FOUND\n", i)
		}
	}
}

func TestFind(t *testing.T) {
	err := openDB("db.rocketsql")
	if err != nil {
		msg := createAndSeedDB()
		if msg != "ok" {
			t.Fatal(msg)
		}
	}

	root, err := loadPage(2)
	if err != nil {
		t.Fatalf("Failed to load page: %s", err)
	}
	for i := 0; i <= 30; i++ {
		data, pg := find(uint32(i), root)
		if pg != dbNullPage {
			fmt.Printf("Page: %d -> Key: %d -> Data: %v\n", pg, i, data)
		}
	}
}

func TestInsert(t *testing.T) {
	err := openDB("db.rocketsql")
	if err != nil {
		msg := createAndSeedDB()
		if msg != "ok" {
			t.Fatal(msg)
		}
	}

	p1, err := loadPageOne()
	if err != nil {
		t.Fatalf("Failed to load page one: %s", err)
	}

	root, err := loadPage(2)
	if err != nil {
		t.Fatalf("Failed to load page: %s", err)
	}

	key := uint32(0)
	payload := []byte{0}
	err = insert(key, payload, root, &p1.firstFreePtr)
	if err != nil {
		t.Fatalf("Failed to insert cell: %s", err)
	}

	err = savePageOne(p1)
	if err != nil {
		t.Fatalf("Failed to save page one to disk: %s", err)
	}

	for i := 0; i <= 30; i++ {
		data, pg := find(uint32(i), root)
		if pg != dbNullPage {
			fmt.Printf("Page: %d -> Key: %d -> Data: %v\n", pg, i, data)
		}
	}
}
