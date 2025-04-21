package pager

import (
	"errors"

	"github.com/m0hossam/rocketsql/file"
	"github.com/m0hossam/rocketsql/page"
)

type Pager struct {
	fileManager *file.FileManager
	newPgPtr    *uint32
}

func NewPager(dbFilePath string) (*Pager, error) {
	fm, err := file.NewFileManager(dbFilePath, page.DefaultPageSize)
	if err != nil {
		return nil, err
	}

	num64, err := fm.GetNumberOfPages()
	if err != nil {
		return nil, err
	}
	num32u := uint32(num64)

	pgr := &Pager{
		fileManager: fm,
		newPgPtr:    &num32u,
	}

	if *pgr.newPgPtr == 0 { // new database
		*pgr.newPgPtr = 1

		pg1, err := page.NewPage(page.LeafPage, pgr.newPgPtr) // schema table
		if err != nil {
			return nil, err
		}

		err = pgr.AppendPage(pg1)
		if err != nil {
			return nil, err
		}

		pg2, err := page.NewPage(page.LeafPage, pgr.newPgPtr) // auto-inc table
		if err != nil {
			return nil, err
		}

		err = pgr.AppendPage(pg2)
		if err != nil {
			return nil, err
		}
	}

	return pgr, nil
}

func (pgr *Pager) ReadPage(ptr uint32) (*page.Page, error) {
	if ptr == 0 { // pages are numbered starting from 1, 0 is reserved for null pages
		return nil, errors.New("page numbers start from 1")
	}

	off := int64((ptr - 1) * page.DefaultPageSize)
	data, err := pgr.fileManager.Read(off)
	if err != nil {
		return nil, err
	}

	return page.DeserializePage(ptr, data), nil
}

func (pgr *Pager) AppendPage(pg *page.Page) error {
	data := pg.SerializePage()
	return pgr.fileManager.Append(data)
}

func (pgr *Pager) WritePage(pg *page.Page) error {
	data := pg.SerializePage()
	off := int64((pg.Id - 1) * page.DefaultPageSize)
	return pgr.fileManager.Write(off, data)
}

func (pgr *Pager) Close() error {
	if pgr.fileManager != nil {
		return pgr.fileManager.Close()
	}
	return nil
}

func (pgr *Pager) GetNewPagePtr() *uint32 {
	return pgr.newPgPtr
}
