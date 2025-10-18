package pager

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/m0hossam/rocketsql/page"
	"github.com/m0hossam/rocketsql/record"
)

type Pager struct {
	dbHeader   *page.DbHeader
	pageBuffer []byte
	newPgPtr   *uint32
}

func NewPager() (*Pager, error) {
	hdr := &page.DbHeader{
		NumPages:      0,
		FirstFreePage: 0,
		NumFreePages:  0,
	}

	newPgPtr := uint32(hdr.NumPages + 1)

	pgr := &Pager{
		dbHeader:   hdr,
		pageBuffer: make([]byte, 1000*page.DefaultPageSize+page.DbHeaderSize),
		newPgPtr:   &newPgPtr,
	}

	return pgr, nil
}

func (pgr *Pager) ReadPage(ptr uint32) (*page.Page, error) {
	if ptr == 0 { // pages are numbered starting from 1, 0 is reserved for null pages
		return nil, errors.New("page numbers start from 1")
	}

	off := (ptr-1)*page.DefaultPageSize + page.DbHeaderSize
	end := off + page.DefaultPageSize

	if int(end) > len(pgr.pageBuffer) {
		return nil, errors.New("page number exceeding file range")
	}

	return page.DeserializePage(ptr, pgr.pageBuffer[off:end]), nil
}

func (pgr *Pager) WritePage(pg *page.Page) error {
	data := pg.SerializePage()

	off := (pg.Id-1)*page.DefaultPageSize + page.DbHeaderSize
	end := off + page.DefaultPageSize

	if int(end) > len(pgr.pageBuffer) {
		return errors.New("page number exceeding file range")
	}

	// New page, increment DB header page count
	if pg.Id > pgr.dbHeader.NumPages {
		pgr.dbHeader.NumPages++
	}

	copy(pgr.pageBuffer[off:end], data)
	return nil
}

func (pgr *Pager) AllocatePage(pType uint8) (*page.Page, error) {
	if pgr.dbHeader.NumFreePages == 0 {
		return page.NewPage(pType, pgr.newPgPtr)
	}

	// Get recycled page from the freelist in the DB header
	pgNo := pgr.dbHeader.FirstFreePage
	pg, err := pgr.ReadPage(pgNo)
	if err != nil {
		return nil, err
	}

	pgr.dbHeader.FirstFreePage = pg.LastPtr // Next free page
	pgr.dbHeader.NumFreePages--

	pg.LastPtr = page.DbNullPage
	pg.Type = pType

	return pg, nil
}

// Returns the number of bytes removed from the end of the DB file
func (pgr *Pager) Vacuum() (int, error) {
	initialNumPages := pgr.dbHeader.NumPages

	// Append all page numbers of free pages to a list
	cur := pgr.dbHeader.FirstFreePage
	freePageNums := make([]uint32, 0)
	for cur != page.DbNullPage {
		freePageNums = append(freePageNums, cur)
		pg, err := pgr.ReadPage(cur)
		if err != nil {
			return 0, err
		}
		cur = pg.LastPtr // Next free page
	}

	// Sort free page numbers in descending order
	// so that we can remove contiguous pages from the end of the DB file
	sort.Slice(freePageNums, func(i, j int) bool {
		return freePageNums[i] > freePageNums[j]
	})

	// Remove contiguous free pages from the end of the DB file
	for _, pgNo := range freePageNums {
		if pgNo != pgr.dbHeader.NumPages { // Must be the last page in the DB
			break
		}

		pgr.dbHeader.NumPages--
		*pgr.newPgPtr--
	}

	// Reset the freelist
	pgr.dbHeader.FirstFreePage = page.DbNullPage
	pgr.dbHeader.NumFreePages = 0

	for _, pgNo := range freePageNums {
		// If page is not at the end of the DB file
		if pgNo < pgr.dbHeader.NumPages {
			pg, err := pgr.ReadPage(pgNo)
			if err != nil {
				return 0, err
			}

			// Add page to the linked-list of free pages
			pg.LastPtr = pgr.dbHeader.FirstFreePage // Store the next free page no. in this page's rightmost pointer
			pgr.dbHeader.FirstFreePage = pgNo
			pgr.dbHeader.NumFreePages++
			if err = pgr.WritePage(pg); err != nil { // Flush page to disk
				return 0, err
			}
		}
	}

	return int(initialNumPages-pgr.dbHeader.NumPages) * page.DefaultPageSize, nil
}

func (pgr *Pager) FreePage(pgNo uint32) error {
	// Last page in DB
	if pgr.dbHeader.NumPages == pgNo {
		pgr.dbHeader.NumPages--
		*pgr.newPgPtr--
		return nil
	}

	pg, err := pgr.ReadPage(pgNo)
	if err != nil {
		return err
	}

	// Add page to the linked-list of free pages
	pg.Truncate()
	pg.LastPtr = pgr.dbHeader.FirstFreePage // Store the next free page no. in this page's rightmost pointer
	pgr.dbHeader.FirstFreePage = pgNo
	pgr.dbHeader.NumFreePages++
	return pgr.WritePage(pg)
}

func (pgr *Pager) GetDbHeader() *page.DbHeader {
	return pgr.dbHeader
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

	return sb.String()
}

func (pgr *Pager) DumpPage(pageNo uint32) string {
	sb := new(strings.Builder)

	pg, err := pgr.ReadPage(pageNo)
	if err != nil {
		return err.Error()
	}
	dumpPage(pg, sb)

	return sb.String()
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

func (pgr *Pager) Close() error {
	pgr.dbHeader = nil
	pgr.pageBuffer = nil
	pgr.newPgPtr = nil
	return nil
}
