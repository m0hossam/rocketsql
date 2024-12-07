package main

import (
	"errors"
	"strconv"
)

var (
	dbFilePath string = "db.rocketsql" // default value
)

func createDB(path string) error {
	err := createFile(path)
	if err != nil {
		return err
	}
	dbFilePath = path

	firstFreePtr := uint32(1)

	pg1, err := createPage(leafPage, &firstFreePtr) // schema table
	if err != nil {
		return err
	}

	pg2, err := createPage(leafPage, &firstFreePtr) // auto-inc table
	if err != nil {
		return err
	}

	err = saveNewPage(pg1)
	if err != nil {
		return err
	}

	err = saveNewPage(pg2)
	if err != nil {
		return err
	}

	// table name - id
	serKey := serializeRow([]string{"VARCHAR(255)"}, []string{"first_free_page"})
	serRow := serializeRow([]string{"VARCHAR(255)", "INT"}, []string{"first_free_page", strconv.Itoa(0)})

	err = insert(pg2, serKey, serRow, &firstFreePtr)
	if err != nil {
		return err
	}

	return nil
}

func openDB(path string) error {
	err := openFile(path)
	if err != nil {
		return err
	}
	dbFilePath = path
	return nil
}

func loadPage(ptr uint32) (*page, error) {
	if ptr == 0 {
		return nil, errors.New("page numbers start from 1")
	}
	b, err := loadPageFromDisk(dbFilePath, ptr)
	if err != nil {
		return nil, err
	}
	p := deserializePage(ptr, b)
	return p, nil
}

func savePage(p *page) error {
	b := serializePage(p)
	return savePageToDisk(dbFilePath, b, p.id)
}

func saveNewPage(p *page) error {
	b := serializePage(p)
	return saveNewPageToDisk(dbFilePath, b)
}
