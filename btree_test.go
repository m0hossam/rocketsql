package main

import (
	"fmt"
	"testing"
)

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
			fmt.Printf("Key %d should be at Page %d", i, pgNum)
			fmt.Printf(" (Trail:")
			for _, num := range path {
				fmt.Printf(" %d", num)
				if num != pgNum {
					fmt.Print(" ->")
				}
			}
			fmt.Println(")")
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

	// INSERTION PART ##################
	key := uint32(7)
	payload := []byte{7}
	err = insert(key, payload, root, &p1.firstFreePtr)
	if err != nil {
		t.Fatalf("Failed to insert cell: %s", err)
	}
	// ###################################

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
