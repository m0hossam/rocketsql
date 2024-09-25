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

	p := createPageOne()
	b := serializePageOne(p)
	return saveNewPageToDisk(dbFilePath, b)
}

func openDB(path string) error {
	err := openFile(path)
	if err != nil {
		return err
	}
	dbFilePath = path
	return nil
}
func loadPageOne() (*pageOne, error) {
	b, err := loadPageFromDisk(dbFilePath, 1)
	if err != nil {
		return nil, err
	}
	p := deserializePageOne(b)
	return p, nil
}

func loadPage(ptr uint32) (*page, error) {
	if ptr < 2 {
		return nil, errors.New("cannot load pages numbered less than 2")
	}
	b, err := loadPageFromDisk(dbFilePath, ptr)
	if err != nil {
		return nil, err
	}
	p := deserializePage(ptr, b)
	return p, nil
}

func savePageOne(p *pageOne) error {
	b := serializePageOne(p)
	return savePageToDisk(dbFilePath, b, 1)
}

func savePage(p *page) error {
	b := serializePage(p)
	return savePageToDisk(dbFilePath, b, p.id)
}

func saveNewPage(p *page) error {
	b := serializePage(p)
	return saveNewPageToDisk(dbFilePath, b)
}
