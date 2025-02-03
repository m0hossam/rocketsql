package storage

import (
	"errors"
	"strconv"
)

var (
	DbFilePath string = "db.rocketsql" // default value
)

type Pager struct {
	dbFilePath string
	cache      map[uint32]*frame
	maxFrames  int
	nFrames    int
	nHits      int
	nMisses    int
}

type frame struct {
	pg      *page
	pins    int
	isDirty bool
}

func CreatePager(dbFilePath string, maxFrames int) *Pager {
	maxFrames = min(maxFrames, 5) // TODO: change to 2000?
	return &Pager{
		dbFilePath: dbFilePath,
		cache:      make(map[uint32]*frame, maxFrames),
		maxFrames:  maxFrames,
		nFrames:    0,
		nHits:      0,
		nMisses:    0,
	}
}

func CreateDb(path string) error {
	err := createFile(path)
	if err != nil {
		return err
	}
	DbFilePath = path

	firstFreePtr := uint32(1)

	pg1, err := CreatePage(LeafPage, &firstFreePtr) // schema table
	if err != nil {
		return err
	}

	pg2, err := CreatePage(LeafPage, &firstFreePtr) // auto-inc table
	if err != nil {
		return err
	}

	err = SaveNewPage(pg1)
	if err != nil {
		return err
	}

	err = SaveNewPage(pg2)
	if err != nil {
		return err
	}

	// table name - id
	serKey := SerializeRow([]string{"VARCHAR(255)"}, []string{"first_free_page"})
	serRow := SerializeRow([]string{"VARCHAR(255)", "INT"}, []string{"first_free_page", strconv.Itoa(0)})

	err = BtreeInsert(pg2, serKey, serRow, &firstFreePtr)
	if err != nil {
		return err
	}

	return nil
}

func OpenDb(path string) error {
	err := openFile(path)
	if err != nil {
		return err
	}
	DbFilePath = path
	return nil
}

func LoadPage(ptr uint32) (*page, error) {
	if ptr == 0 {
		return nil, errors.New("page numbers start from 1")
	}
	b, err := loadPageFromDisk(DbFilePath, ptr)
	if err != nil {
		return nil, err
	}
	p := deserializePage(ptr, b)
	return p, nil
}

func SavePage(p *page) error {
	b := serializePage(p)
	return savePageToDisk(DbFilePath, b, p.id)
}

func SaveNewPage(p *page) error {
	b := serializePage(p)
	return saveNewPageToDisk(DbFilePath, b)
}
