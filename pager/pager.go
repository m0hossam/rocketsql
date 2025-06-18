package pager

import (
	"errors"

	"github.com/m0hossam/rocketsql/page"
)

type Pager struct {
	pageBuffer []byte
	newPgPtr   *uint32
}

func NewPager() (*Pager, error) {
	num32u := uint32(0) // Indicates new DB, will be incremented by B-Tree module before creating the schema table

	pgr := &Pager{
		pageBuffer: make([]byte, 0, 4096),
		newPgPtr:   &num32u,
	}

	return pgr, nil
}

func (pgr *Pager) ReadPage(ptr uint32) (*page.Page, error) {
	if ptr == 0 { // Pages are numbered starting from 1, 0 is reserved for null pages
		return nil, errors.New("page numbers start from 1")
	}

	off := (ptr - 1) * page.DefaultPageSize
	end := off + page.DefaultPageSize

	if int(end) > len(pgr.pageBuffer) {
		return nil, errors.New("page number exceeding file range")
	}

	return page.DeserializePage(ptr, pgr.pageBuffer[off:off+end]), nil
}

func (pgr *Pager) AppendPage(pg *page.Page) error {
	data := pg.SerializePage()
	pgr.pageBuffer = append(pgr.pageBuffer, data...)
	return nil
}

func (pgr *Pager) WritePage(pg *page.Page) error {
	data := pg.SerializePage()
	off := (pg.Id - 1) * page.DefaultPageSize
	end := off + page.DefaultPageSize
	copy(pgr.pageBuffer[off:off+end], data)
	return nil
}

func (pgr *Pager) Close() error {
	pgr.pageBuffer = nil
	return nil
}

func (pgr *Pager) GetNewPagePtr() *uint32 {
	return pgr.newPgPtr
}

func (pgr *Pager) IncNewPagePtr() {
	*pgr.newPgPtr++
}
