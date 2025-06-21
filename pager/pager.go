package pager

import (
	"errors"
	"fmt"
	"strings"

	"github.com/m0hossam/rocketsql/file"
	"github.com/m0hossam/rocketsql/page"
	"github.com/m0hossam/rocketsql/record"
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

	if num32u != 0 { // Not a new database
		num32u++
	}

	pgr := &Pager{
		fileManager: fm,
		newPgPtr:    &num32u,
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

func (pgr *Pager) IncNewPagePtr() {
	*pgr.newPgPtr++
}

func (pgr *Pager) DumpTable(tblName string, rootPgNo uint32) string {
	sb := new(strings.Builder)

	// Generic BFS
	q := []uint32{}
	level := 1
	q = append(q, rootPgNo)
	for len(q) != 0 {
		levelSz := len(q)
		fmt.Fprintf(sb, "******************* LEVEL %d *******************\n", level)
		for levelSz != 0 {
			levelSz--
			pgId := q[0]
			q = q[1:] // dequeue

			pg, err := pgr.ReadPage(pgId)
			if err != nil {
				return err.Error()
			}
			dumpPage(pg, sb)
			if pg.Type == page.InteriorPage {
				for i := 0; i < len(pg.CellPtrArr); i++ {
					q = append(q, page.BytesToUint32(pg.Cells[pg.CellPtrArr[i]].Value)) // enqueue children
				}
				q = append(q, pg.LastPtr)
			}
		}
		level++
	}

	if err := pgr.fileManager.WriteTo(fmt.Sprintf("table_%s_dump.txt", tblName), sb.String()); err != nil {
		return err.Error()
	}
	return fmt.Sprintf("Table '%s' dumped to 'table_%s_dump.txt'", tblName, tblName)
}

func (pgr *Pager) DumpPage(pageNo uint32) string {
	sb := new(strings.Builder)

	pg, err := pgr.ReadPage(pageNo)
	if err != nil {
		return err.Error()
	}
	dumpPage(pg, sb)

	if err := pgr.fileManager.WriteTo(fmt.Sprintf("page_%d_dump.txt", pageNo), sb.String()); err != nil {
		return err.Error()
	}

	return fmt.Sprintf("Page %d dumped to 'page_%d_dump.txt'", pageNo, pageNo)
}

func dumpPage(pg *page.Page, sb *strings.Builder) {
	sb.WriteString("#############################\n")
	fmt.Fprintf(sb, "ID: %d\n", pg.Id)
	if pg.Type == page.LeafPage {
		sb.WriteString("Type: Leaf\n")
	} else {
		sb.WriteString("Type: Interior\n")
	}
	if pg.FreeList != nil {
		fmt.Fprintf(sb, "Offset of first free block: %d\n", pg.FreeList.Offset)
	} else {
		sb.WriteString("Offset of first free block: NO FREE BLOCKS\n")
	}
	fmt.Fprintf(sb, "No. of Cells: %d\n", pg.NumCells)
	fmt.Fprintf(sb, "Offset of cell array region: %d\n", pg.CellArrOff)
	fmt.Fprintf(sb, "No. of fragmented bytes: %d\n", pg.NumFragBytes)

	for i := 0; i < len(pg.CellPtrArr); i++ {
		c := pg.Cells[pg.CellPtrArr[i]]
		fmt.Fprintf(sb, "\tCell[%d]:\n", i)
		size := 2 + len(c.Key) + len(c.Value)
		if pg.Type == page.InteriorPage {
			fmt.Fprintf(sb, "\t\tPtr: %d\n", page.BytesToUint32(c.Value))
		}
		keyRec, _ := record.NewRecord(c.Key)
		fmt.Fprintf(sb, "\t\tKey: %s\n", keyRec.ToString())
		if pg.Type == page.LeafPage {
			valRec, _ := record.NewRecord(c.Value)
			fmt.Fprintf(sb, "\t\tRow: %s\n", valRec.ToString())
			size += 2
		}
		fmt.Fprintf(sb, "\t\tStart: %d\n", pg.CellPtrArr[i])
		fmt.Fprintf(sb, "\t\tEnd: %d\n", int(pg.CellPtrArr[i])+size)
		fmt.Fprintf(sb, "\t\tSize: %d\n", size)

	}
	fmt.Fprintf(sb, "Rightmost Ptr: %d\n", pg.LastPtr)
}
