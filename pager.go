package main

import "errors"

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
	pg1, err := createPage(leafPage, &firstFreePtr)
	if err != nil {
		return err
	}

	err = saveNewPage(pg1)
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
