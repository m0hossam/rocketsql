package storage

import (
	"errors"
	"strconv"
	"sync/atomic"
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
	pg   *page
	pins int
}

func (pgr *Pager) GetPagerHits() int {
	return pgr.nHits
}

func (pgr *Pager) GetPagerMisses() int {
	return pgr.nMisses
}

func CreatePager(dbFilePath string, maxFrames int) *Pager {
	maxFrames = min(maxFrames, 2000) // TODO: put value in a const instead of hardcoding it
	return &Pager{
		dbFilePath: dbFilePath,
		cache:      make(map[uint32]*frame, maxFrames),
		maxFrames:  maxFrames,
		nFrames:    0,
		nHits:      0,
		nMisses:    0,
	}
}

func CreateDb(path string, btree *Btree) error {
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

	err = btree.pgr.SaveNewPage(pg1)
	if err != nil {
		return err
	}

	err = btree.pgr.SaveNewPage(pg2)
	if err != nil {
		return err
	}

	// table name - id
	serKey := SerializeRow([]string{"VARCHAR(255)"}, []string{"first_free_page"})
	serRow := SerializeRow([]string{"VARCHAR(255)", "INT"}, []string{"first_free_page", strconv.Itoa(0)})

	err = btree.BtreeInsert(pg2, serKey, serRow, &firstFreePtr)
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

func CreatePage(pType uint8, firstFreePtr *uint32) (*page, error) {
	if pType != InteriorPage && pType != LeafPage { // invalid type
		return nil, errors.New("invalid page type")
	}
	p := &page{
		id:         *firstFreePtr,
		pType:      pType,
		nCells:     0,
		cellArrOff: dbPageSize,
		nFragBytes: 0,
		lastPtr:    DbNullPage,
		cellPtrArr: []uint16{},
		cells:      map[uint16]cell{},
	}
	atomic.AddUint32(firstFreePtr, 1)
	return p, nil
}

func (pgr *Pager) LoadPage(ptr uint32) (*page, error) {
	// pages are numbered starting from 1, 0 is reserved for null pages
	if ptr == 0 {
		return nil, errors.New("page numbers start from 1")
	}

	// try to get page from cache
	frm, hit := pgr.cache[ptr]
	if hit {
		frm.pins++
		pgr.nHits++
		return frm.pg, nil
	}

	// cache miss
	pgr.nMisses++

	// load page from disk
	b, err := loadPageFromDisk(DbFilePath, ptr)
	if err != nil {
		return nil, err
	}
	p := deserializePage(ptr, b)

	// try to put page into cache
	if pgr.nFrames < pgr.maxFrames {
		pgr.nFrames++
		pgr.cache[ptr] = &frame{
			pg:   p,
			pins: 1,
		}
	}

	return p, nil
}

func (pgr *Pager) SaveNewPage(p *page) error {
	b := serializePage(p)
	err := saveNewPageToDisk(DbFilePath, b)
	if err == nil {
		if pgr.nFrames < pgr.maxFrames {
			pgr.nFrames++
			pgr.cache[p.id] = &frame{
				pg:   p,
				pins: 0,
			}
		}
	}
	return err
}

func (pgr *Pager) SavePage(p *page) error {
	b := serializePage(p)

	err := savePageToDisk(DbFilePath, b, p.id)
	if err != nil {
		frm, hit := pgr.cache[p.id]
		if hit {
			frm.pins--
		}
	}
	return err
}
