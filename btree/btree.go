package btree

import (
	"errors"

	"github.com/m0hossam/rocketsql/page"
	"github.com/m0hossam/rocketsql/pager"
	"github.com/m0hossam/rocketsql/parser"
	"github.com/m0hossam/rocketsql/record"
)

/*
Simplifying assumptions:
- Max cell size is designed such that at least 2 cells can fit in a page
- to avoid cases where we need to create two new pages for insertion
*/

type BtreeIterator struct {
	pgr     *pager.Pager
	curPg   *page.Page
	curSlot int
}

type Btree struct {
	pgr *pager.Pager
}

func NewBtree(dbFilePath string) (*Btree, error) {
	pgr, err := pager.NewPager(dbFilePath)
	if err != nil {
		return nil, err
	}

	btree := &Btree{
		pgr: pgr,
	}

	if *pgr.GetNewPagePtr() == 0 { // New database
		// Create rocketsql_schema table
		pgr.IncNewPagePtr()
		pg1, err := page.NewPage(page.LeafPage, pgr.GetNewPagePtr())
		if err != nil {
			return nil, err
		}
		if err = pgr.AppendPage(pg1); err != nil {
			return nil, err
		}

		// Insert schema_table data into schema_table
		keyRec := &record.Record{
			Columns: []*parser.TypeDef{{Type: "VARCHAR", Size: 255}},
			Values:  []*parser.Constant{{Type: parser.StringToken, StrVal: "rocketsql_schema"}},
		}
		valueRec := &record.Record{
			Columns: []*parser.TypeDef{
				{Type: "VARCHAR", Size: 255},
				{Type: "INT"},
				{Type: "VARCHAR", Size: 255}},
			Values: []*parser.Constant{
				{Type: parser.StringToken, StrVal: "rocketsql_schema"},
				{Type: parser.IntegerToken, IntVal: 1},
				{Type: parser.StringToken, StrVal: "CREATE TABLE rocketsql_schema (table_name VARCHAR(255), root_page_no INT, table_schema VARCHAR(255))"}},
		}

		key, err := keyRec.Serialize()
		if err != nil {
			return nil, err
		}
		value, err := valueRec.Serialize()
		if err != nil {
			return nil, err
		}

		if err = btree.Insert(1, key, value); err != nil {
			return nil, err
		}
	}

	return btree, nil
}

func newBtreeIterator(pgr *pager.Pager, pg *page.Page, idx int) *BtreeIterator {
	return &BtreeIterator{
		pgr:     pgr,
		curPg:   pg,
		curSlot: idx,
	}
}

// returns row, ok
func (it *BtreeIterator) Next() ([]byte, bool, error) {
	for it.curSlot >= int(it.curPg.NumCells) {
		if it.curPg.LastPtr == page.DbNullPage {
			return nil, false, nil
		}

		pg, err := it.pgr.ReadPage(it.curPg.LastPtr)
		if err != nil {
			return nil, true, err
		}

		it.curPg = pg
		it.curSlot = 0
	}
	off := it.curPg.CellPtrArr[it.curSlot]
	data := it.curPg.Cells[off].Value
	it.curSlot++
	return data, true, nil
}

func (it *BtreeIterator) GetKey() []byte {
	off := it.curPg.CellPtrArr[it.curSlot-1]
	return it.curPg.Cells[off].Key
}

func upperBoundIndex(pg *page.Page, key []byte) (int, error) { // returns index of 1st cell with a key > new key, or the no. of cells if none found
	l := 0
	r := int(pg.NumCells) - 1
	ind := int(pg.NumCells)
	for l <= r {
		m := (l + r) / 2
		c := pg.Cells[pg.CellPtrArr[m]]
		k := c.Key
		cmp := record.Compare(k, key)
		if cmp == record.FirstEqualSecond {
			return 0, errors.New("the key is not unique")
		}
		if cmp == record.FirstLessThanSecond {
			l = m + 1
		}
		if cmp == record.FirstGreaterThanSecond {
			ind = m
			r = m - 1
		}
	}
	return ind, nil
}

func insertCell(pg *page.Page, c page.Cell, ind int, newOff uint16) {
	pg.CellPtrArr = append(pg.CellPtrArr, 0)
	for i := len(pg.CellPtrArr) - 1; i > ind; i-- { // shifting
		pg.CellPtrArr[i] = pg.CellPtrArr[i-1]
	}
	pg.CellPtrArr[ind] = newOff

	pg.Cells[newOff] = c
	pg.NumCells++

	if newOff < pg.CellArrOff { // keep track of closest cell to the top of the page
		pg.CellArrOff = newOff
	}
}

func defragPage(pg *page.Page) {
	cells := []page.Cell{}
	ptr := pg.LastPtr
	for _, off := range pg.CellPtrArr {
		cells = append(cells, pg.Cells[off])
	}
	pg.Truncate()

	pg.LastPtr = ptr
	newOff := uint16(page.DefaultPageSize)
	for i, c := range cells {
		cellSize := uint16(2 + len(c.Key) + len(c.Value))
		if pg.Type == page.LeafPage {
			cellSize += 2
		}
		newOff -= cellSize
		insertCell(pg, c, i, newOff) // should always succeed
	}
}

func insertIntoPage(pg *page.Page, c page.Cell, ind int) error {
	if pg.NumCells == page.MaxCellsPerPage {
		return errors.New("page does not have enough space (soft limit), need to split")
	}

	cellSize := uint16(2 + len(c.Key) + len(c.Value))
	if pg.Type == page.LeafPage {
		cellSize += 2
	}

	unallocatedSpace := pg.CellArrOff - (page.OffsetOfCellPtrArr + page.SizeOfCellOff*pg.NumCells) // space between last cell ptr and first cell
	totalFreeSize := uint16(unallocatedSpace) + uint16(pg.NumFragBytes)
	for head := pg.FreeList; head != nil; head = head.Next {
		totalFreeSize += head.Size
	}

	if totalFreeSize < cellSize+page.SizeOfCellOff {
		return errors.New("page does not have enough space, need to split")
	}

	if unallocatedSpace < page.SizeOfCellOff {
		defragPage(pg)
		unallocatedSpace = pg.CellArrOff - (page.OffsetOfCellPtrArr + page.SizeOfCellOff*pg.NumCells) // variable must be recalculated here because it's used again
	}

	var prev *page.FreeBlock = nil
	head := pg.FreeList
	for head != nil { // first-fit singly linked list traversal
		if cellSize <= head.Size {
			remSz := head.Size - cellSize
			if remSz >= page.MinFreeBlockSize { // new block takes upper half of old block
				head.Size = remSz
				newOff := head.Offset + remSz
				insertCell(pg, c, ind, newOff) // should always succeed
			} else { // new frag takes lower half
				if prev != nil {
					prev.Next = head.Next // remove the current block
				} else { // remove head
					pg.FreeList = nil
				}
				newOff := head.Offset
				insertCell(pg, c, ind, newOff)             // should always succeed
				if int(pg.NumFragBytes)+int(remSz) > 255 { // uint8 overflow precaution
					defragPage(pg)
				} else {
					pg.NumFragBytes += uint8(remSz)
				}
			}
			return nil
		}

		prev = head
		head = head.Next
	}

	if unallocatedSpace < cellSize+page.SizeOfCellOff {
		defragPage(pg)
	}

	newOff := pg.CellArrOff - cellSize
	insertCell(pg, c, ind, newOff) // should always succeed

	return nil
}

func getOverfullCellArr(pg *page.Page, newCell page.Cell, ind int) []page.Cell {
	cells := []page.Cell{}
	for i := 0; i < ind; i++ {
		c := pg.Cells[pg.CellPtrArr[i]]
		cells = append(cells, c)
	}
	cells = append(cells, newCell)
	for i := ind; i < len(pg.CellPtrArr); i++ {
		c := pg.Cells[pg.CellPtrArr[i]]
		cells = append(cells, c)
	}

	return cells
}

func insertFreeBlock(pg *page.Page, newFreeBlock *page.FreeBlock) {
	if pg.FreeList == nil || newFreeBlock.Offset < pg.FreeList.Offset {
		newFreeBlock.Next = pg.FreeList
		pg.FreeList = newFreeBlock
	} else {
		cur := pg.FreeList
		for cur.Next != nil && cur.Next.Offset < newFreeBlock.Offset {
			cur = cur.Next
		}
		newFreeBlock.Next = cur.Next
		cur.Next = newFreeBlock
	}

	cur := pg.FreeList
	for cur != nil && cur.Next != nil {
		if cur.Offset+cur.Size == cur.Next.Offset { // merge contiguous blocks
			cur.Size += cur.Next.Size
			cur.Next = cur.Next.Next
		} else {
			cur = cur.Next
		}
	}
}

func removeCell(pg *page.Page, ind int) {
	off := pg.CellPtrArr[ind]
	c := pg.Cells[off]
	cellSize := uint16(2 + len(c.Key) + len(c.Value))
	if pg.Type == page.LeafPage {
		cellSize += 2
	}

	delete(pg.Cells, off)
	pg.CellPtrArr = append(pg.CellPtrArr[:ind], pg.CellPtrArr[ind+1:]...)
	pg.NumCells--

	if off == pg.CellArrOff {
		pg.CellArrOff += cellSize
	} else {
		newFreeBlock := &page.FreeBlock{
			Offset: off,
			Size:   cellSize,
		}
		insertFreeBlock(pg, newFreeBlock)
	}
}

func (btree *Btree) getPath(key []byte, root *page.Page) []uint32 { // returns page numbers from the root of the table the to leaf that should contain the key
	cur := root
	path := []uint32{root.Id}
	for cur.Type != page.LeafPage { // find the leaf page that should contain the key, takes O(H * log(N)) where H is the tree height and N is the max no. of keys in a page
		l := 0
		r := int(cur.NumCells) - 1
		ind := int(cur.NumCells)
		for l <= r { // binary search
			m := (l + r) / 2
			c := cur.Cells[cur.CellPtrArr[m]]
			k := c.Key
			cmp := record.Compare(k, key)
			if cmp == record.FirstEqualSecond {
				ind = m + 1
				break
			}
			if cmp == record.FirstLessThanSecond {
				l = m + 1
			}
			if cmp == record.FirstGreaterThanSecond {
				ind = m
				r = m - 1
			}
		}

		ptr := page.DbNullPage
		if ind == int(cur.NumCells) {
			ptr = cur.LastPtr
		} else {
			c := cur.Cells[cur.CellPtrArr[ind]]
			ptr = page.DeserializePtr(c.Value)
		}

		var err error
		cur, err = btree.pgr.ReadPage(ptr)
		if err != nil {
			return nil
		}
		path = append(path, cur.Id)
	}
	return path
}

func (btree *Btree) interiorInsert(path []uint32, key []byte, value []byte, newChild uint32, newPgPtr *uint32) error {
	if (len(key) + len(value) + 2) > page.MaxCellSize {
		return errors.New("max cell size exceeded")
	}

	if len(path) == 0 { // creating new root
		rootPgNo := page.DeserializePtr(value)
		rootPg, err := btree.pgr.ReadPage(rootPgNo)
		if err != nil {
			return err
		}

		newPg, err := page.NewPage(rootPg.Type, newPgPtr)
		if err != nil {
			return err
		}

		newCell := page.Cell{
			Key:   key,
			Value: page.SerializePtr(newPg.Id), // because value will point to the root
		}

		rootPg.CopyTo(newPg)
		rootPg.Truncate()
		rootPg.Type = page.InteriorPage
		rootPg.LastPtr = newChild
		insertCell(rootPg, newCell, 0, uint16(page.DefaultPageSize-len(newCell.Key)-len(newCell.Value)-2))

		err = btree.pgr.AppendPage(newPg)
		if err != nil {
			return err
		}
		err = btree.pgr.WritePage(rootPg)
		if err != nil {
			return err
		}

		return nil
	}

	newCell := page.Cell{
		Key:   key,
		Value: value,
	}
	pg, err := btree.pgr.ReadPage(path[len(path)-1])
	if err != nil {
		return err
	}
	ind, err := upperBoundIndex(pg, key)
	if err != nil {
		return err
	}
	if ind == int(pg.NumCells) {
		pg.LastPtr = newChild
	} else {
		c := pg.Cells[pg.CellPtrArr[ind]]
		c.Value = page.SerializePtr(newChild)
		pg.Cells[pg.CellPtrArr[ind]] = c
	}
	err = insertIntoPage(pg, newCell, ind)
	if err == nil {
		return btree.pgr.WritePage(pg)
	}

	cells := getOverfullCellArr(pg, newCell, ind)
	mid := len(cells) / 2

	newPg, err := page.NewPage(page.InteriorPage, newPgPtr)
	if err != nil {
		return err
	}
	newPg.LastPtr = newChild

	pg.Truncate()
	pg.LastPtr = page.DeserializePtr(cells[mid].Value)

	for i := 0; i < mid; i++ {
		err = insertIntoPage(pg, cells[i], i)
		if err != nil {
			return err
		}
	}
	for i := mid + 1; i < len(cells); i++ {
		err = insertIntoPage(newPg, cells[i], i-mid-1)
		if err != nil {
			return err
		}
	}

	err = btree.pgr.WritePage(pg)
	if err != nil {
		return err
	}
	err = btree.pgr.AppendPage(newPg)
	if err != nil {
		return err
	}

	err = btree.interiorInsert(path[:len(path)-1], cells[mid].Key, page.SerializePtr(pg.Id), newPg.Id, newPgPtr)
	if err != nil {
		return err
	}

	return nil
}

func (btree *Btree) Create(serTblKey []byte, serTblData []byte) error {
	// Create root page of the new table.
	rootPg, err := page.NewPage(page.LeafPage, btree.pgr.GetNewPagePtr())
	if err != nil {
		return err
	}

	// Append the new root page to the database file.
	err = btree.pgr.AppendPage(rootPg)
	if err != nil {
		return err
	}

	// Insert the serialized table key and data into the metadata table.
	return btree.Insert(1, serTblKey, serTblData)
}

func (btree *Btree) First(rootPgNo uint32) (*BtreeIterator, error) {
	rootPg, err := btree.pgr.ReadPage(rootPgNo)
	if err != nil {
		return nil, err
	}

	for rootPg.Type != page.LeafPage {
		ptr := page.DeserializePtr(rootPg.Cells[rootPg.CellPtrArr[0]].Value)
		pg, err := btree.pgr.ReadPage(ptr)
		if err != nil {
			return nil, err
		}
		rootPg = pg
	}
	return newBtreeIterator(btree.pgr, rootPg, 0), nil
}

func (btree *Btree) Get(key []byte, rootPgNo uint32) ([]byte, uint32) { // returns raw tuple data and the containing page number
	rootPg, err := btree.pgr.ReadPage(rootPgNo)
	if err != nil {
		return nil, page.DbNullPage
	}

	path := btree.getPath(key, rootPg)
	ptr := path[len(path)-1]
	pg, err := btree.pgr.ReadPage(ptr)
	if err != nil {
		return nil, page.DbNullPage
	}

	l := 0
	r := int(pg.NumCells) - 1
	for l <= r { // binary search
		m := (l + r) / 2
		c := pg.Cells[pg.CellPtrArr[m]]
		k := c.Key
		cmp := record.Compare(k, key)
		if cmp == record.FirstEqualSecond {
			return c.Value, pg.Id
		}
		if cmp == record.FirstLessThanSecond {
			l = m + 1
		}
		if cmp == record.FirstGreaterThanSecond {
			r = m - 1
		}
	}
	return nil, page.DbNullPage
}

func (btree *Btree) Insert(rootPgNo uint32, key []byte, value []byte) error {
	rootPg, err := btree.pgr.ReadPage(rootPgNo)
	if err != nil {
		return err
	}

	if (len(key) + len(value) + 4) > page.MaxCellSize {
		return errors.New("max cell size exceeded")
	}

	newCell := page.Cell{
		Key:   key,
		Value: value,
	}
	path := btree.getPath(key, rootPg)
	pg, err := btree.pgr.ReadPage(path[len(path)-1])
	if err != nil {
		return err
	}
	ind, err := upperBoundIndex(pg, key)
	if err != nil {
		return err
	}
	err = insertIntoPage(pg, newCell, ind)
	if err == nil {
		return btree.pgr.WritePage(pg)
	}

	cells := getOverfullCellArr(pg, newCell, ind)
	mid := (len(cells) + 1) / 2

	newPg, err := page.NewPage(page.LeafPage, btree.pgr.GetNewPagePtr())
	if err != nil {
		return err
	}
	newPg.LastPtr = pg.LastPtr

	pg.Truncate()
	pg.LastPtr = newPg.Id

	for i := 0; i < mid; i++ {
		err = insertIntoPage(pg, cells[i], i)
		if err != nil {
			return err
		}
	}
	for i := mid; i < len(cells); i++ {
		err = insertIntoPage(newPg, cells[i], i-mid)
		if err != nil {
			return err
		}
	}

	err = btree.pgr.WritePage(pg)
	if err != nil {
		return err
	}
	err = btree.pgr.AppendPage(newPg)
	if err != nil {
		return err
	}

	err = btree.interiorInsert(path[:len(path)-1], newPg.Cells[newPg.CellPtrArr[0]].Key, page.SerializePtr(pg.Id), newPg.Id, btree.pgr.GetNewPagePtr())
	if err != nil {
		return err
	}

	return nil
}

func (btree *Btree) Delete(rootPgNo uint32, key []byte) error {
	rootPg, err := btree.pgr.ReadPage(rootPgNo)
	if err != nil {
		return err
	}

	path := btree.getPath(key, rootPg)
	ptr := path[len(path)-1]
	pg, err := btree.pgr.ReadPage(ptr)
	if err != nil {
		return err
	}

	l := 0
	r := int(pg.NumCells) - 1
	ind := -1
	for l <= r { // binary search
		m := (l + r) / 2
		c := pg.Cells[pg.CellPtrArr[m]]
		k := c.Key
		cmp := record.Compare(k, key)
		if cmp == record.FirstEqualSecond {
			ind = m
			break
		}
		if cmp == record.FirstLessThanSecond {
			l = m + 1
		}
		if cmp == record.FirstGreaterThanSecond {
			r = m - 1
		}
	}

	if ind == -1 {
		return errors.New("key not found")
	}

	removeCell(pg, ind)
	err = btree.pgr.WritePage(pg)
	if err != nil {
		return err
	}

	return nil
}

func (btree *Btree) Close() error {
	if btree.pgr != nil {
		return btree.pgr.Close()
	}
	return nil
}

func (btree *Btree) GetNewPagePtr() *uint32 {
	return btree.pgr.GetNewPagePtr()
}
