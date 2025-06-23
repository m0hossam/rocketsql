package btree

import (
	"errors"

	"github.com/m0hossam/rocketsql/page"
	"github.com/m0hossam/rocketsql/pager"
	"github.com/m0hossam/rocketsql/record"
)

/*
Simplifying assumptions:
- Max cell size is designed such that at least 2 cells can fit in a page
- to avoid cases where we need to create two new pages for insertion
*/

const (
	dropFlag = iota
	truncateFlag
	rebuildFlag
)

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

	if pgr.GetDbHeader().NumPages == 0 { // New database
		// Create rocketsql_schema table
		pg1, err := pgr.AllocatePage(page.LeafPage)
		if err != nil {
			return nil, err
		}
		if err = pgr.WritePage(pg1); err != nil {
			return nil, err
		}

		// Insert schema_table data into schema_table
		keyRec := record.NewSchemaKeyRecord("rocketsql_schema")
		key, err := keyRec.Serialize()
		if err != nil {
			return nil, err
		}

		valueRec := record.NewSchemaValueRecord("rocketsql_schema", 1, "CREATE TABLE rocketsql_schema (table_name VARCHAR(255), root_page_no INT, table_schema VARCHAR(255))")
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

// Returns the overfull cell array and the middle index used for distribution during insertion
func distributeCells(pg *page.Page, newCell page.Cell, newCellIndex int) ([]page.Cell, int) {
	cells := []page.Cell{}
	middleIndex := 0
	pgSpace := page.DefaultPageSize - page.DbHeaderSize // Total empty space in the old page
	cellVarSizes := 2                                   // 2 bytes to determine size of variable key
	if pg.Type == page.LeafPage {
		cellVarSizes += 2 // 2 bytes to determine size of variable value in leaves
	}

	for i := 0; i < newCellIndex; i++ {
		cell := pg.Cells[pg.CellPtrArr[i]]
		cells = append(cells, cell)

		requiredSpace := len(cell.Key) + len(cell.Value) + cellVarSizes + page.SizeOfCellOff

		if middleIndex == 0 { // If we haven't found a middle index yet (if we find it, it'll be non-zero)
			if requiredSpace <= pgSpace { // Fit as many cells as possible in the old page
				pgSpace -= requiredSpace
			} else {
				middleIndex = i
			}
		}
	}

	cells = append(cells, newCell)
	requiredSpace := len(newCell.Key) + len(newCell.Value) + cellVarSizes + page.SizeOfCellOff

	if middleIndex == 0 {
		if requiredSpace <= pgSpace {
			pgSpace -= requiredSpace
		} else {
			middleIndex = newCellIndex
		}
	}

	for i := newCellIndex; i < len(pg.CellPtrArr); i++ {
		cell := pg.Cells[pg.CellPtrArr[i]]
		cells = append(cells, cell)

		requiredSpace = len(cell.Key) + len(cell.Value) + cellVarSizes + page.SizeOfCellOff

		if middleIndex == 0 {
			if requiredSpace <= pgSpace {
				pgSpace -= requiredSpace
			} else {
				middleIndex = i + 1 // We add one because we just appended the new cell before this loop
			}
		}
	}

	// Remember: In interiorInsert, cell[middleIndex] gets inserted into the parent and not into the new page.
	// We need to make sure (middleIndex != last index); because that will lead to an empty new page.
	if middleIndex == (len(cells)-1) && pg.Type == page.InteriorPage {
		middleIndex -= 1 // Remember that (len(cells) >= 3) due to the minCells and MaxCellSize constraints
	}

	return cells, middleIndex
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
			ptr = page.BytesToUint32(c.Value)
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

func (btree *Btree) interiorInsert(path []uint32, key []byte, value []byte, newChild uint32) error {
	if (len(key) + len(value) + 2) > page.MaxCellSize {
		return errors.New("max cell size exceeded")
	}

	if len(path) == 0 { // creating new root
		rootPgNo := page.BytesToUint32(value)
		rootPg, err := btree.pgr.ReadPage(rootPgNo)
		if err != nil {
			return err
		}

		newPg, err := btree.pgr.AllocatePage(rootPg.Type)
		if err != nil {
			return err
		}

		newCell := page.Cell{
			Key:   key,
			Value: page.Uint32ToBytes(newPg.Id), // because value will point to the root
		}

		rootPg.CopyTo(newPg)
		rootPg.Truncate()
		rootPg.Type = page.InteriorPage
		rootPg.LastPtr = newChild
		insertCell(rootPg, newCell, 0, uint16(page.DefaultPageSize-len(newCell.Key)-len(newCell.Value)-2))

		err = btree.pgr.WritePage(newPg)
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
		c.Value = page.Uint32ToBytes(newChild)
		pg.Cells[pg.CellPtrArr[ind]] = c
	}
	err = insertIntoPage(pg, newCell, ind)
	if err == nil {
		return btree.pgr.WritePage(pg)
	}

	cells, mid := distributeCells(pg, newCell, ind)

	newPg, err := btree.pgr.AllocatePage(page.InteriorPage)
	if err != nil {
		return err
	}
	newPg.LastPtr = newChild

	pg.Truncate()
	pg.LastPtr = page.BytesToUint32(cells[mid].Value)

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
	err = btree.pgr.WritePage(newPg)
	if err != nil {
		return err
	}

	err = btree.interiorInsert(path[:len(path)-1], cells[mid].Key, page.Uint32ToBytes(pg.Id), newPg.Id)
	if err != nil {
		return err
	}

	return nil
}

// Returns the root page no. of the table created
func (btree *Btree) Create() (uint32, error) {
	// Create root page of the new table.
	rootPg, err := btree.pgr.AllocatePage(page.LeafPage)
	if err != nil {
		return 0, err
	}

	// Write root page to the database file.
	if err = btree.pgr.WritePage(rootPg); err != nil {
		return 0, err
	}

	return rootPg.Id, nil
}

func (btree *Btree) First(rootPgNo uint32) (*BtreeIterator, error) {
	rootPg, err := btree.pgr.ReadPage(rootPgNo)
	if err != nil {
		return nil, err
	}

	for rootPg.Type != page.LeafPage {
		ptr := page.BytesToUint32(rootPg.Cells[rootPg.CellPtrArr[0]].Value)
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

	cells, mid := distributeCells(pg, newCell, ind)

	newPg, err := btree.pgr.AllocatePage(page.LeafPage)
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
	err = btree.pgr.WritePage(newPg)
	if err != nil {
		return err
	}

	err = btree.interiorInsert(path[:len(path)-1], newPg.Cells[newPg.CellPtrArr[0]].Key, page.Uint32ToBytes(pg.Id), newPg.Id)
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

func (btree *Btree) RebuildTree(rootPgNo uint32) error {

	// First, free all pages in the tree except the root, and buffer the leaf cells
	leafCells, _, err := btree.freeTree(rootPgNo, rebuildFlag)
	if err != nil {
		return err
	}

	// Second, insert the buffered leaf cells into the tree (which now consists of a root only)
	for _, cell := range leafCells {
		if err := btree.Insert(rootPgNo, cell.Key, cell.Value); err != nil {
			return err
		}
	}

	return nil
}

// Returns number of deleted rows, frees the tree's root
func (btree *Btree) DeleteTree(rootPgNo uint32) (int, error) {
	_, delRows, err := btree.freeTree(rootPgNo, dropFlag)
	return delRows, err
}

// Returns number of deleted rows, truncates the tree's root but does not free it
func (btree *Btree) TruncateTree(rootPgNo uint32) (int, error) {
	_, delRows, err := btree.freeTree(rootPgNo, truncateFlag)
	return delRows, err
}

/*
General purpose tree deletion
(flag == dropFlag) -> DROP TABLE t
(flag == truncateFlag) -> TRUNCATE TABLE t || DELETE FROM t
(flag == rebuildFlag) -> .rebuild_table t
*/
func (btree *Btree) freeTree(rootPgNo uint32, flag int) ([]page.Cell, int, error) {
	leafCells := []page.Cell{}
	numRows := 0
	// Generic BFS
	queue := []uint32{}
	queue = append(queue, rootPgNo)
	for len(queue) != 0 {
		levelSz := len(queue)
		for levelSz != 0 {

			levelSz--
			pg, err := btree.pgr.ReadPage(queue[0])
			if err != nil {
				return nil, 0, err
			}
			queue = queue[1:] // dequeue

			if pg.Type == page.InteriorPage {
				for i := 0; i < len(pg.CellPtrArr); i++ {
					queue = append(queue, page.BytesToUint32(pg.Cells[pg.CellPtrArr[i]].Value)) // enqueue children
				}
				queue = append(queue, pg.LastPtr)
			} else if flag == rebuildFlag { // Leaf page and we're rebuilding the tree
				for _, cell := range pg.Cells {
					leafCells = append(leafCells, cell)
				}
			} else { // Leaf page and we're dropping or truncating the tree
				numRows += int(pg.NumCells)
			}

			// If we're truncating or rebuilding the tree, the root isn't freed, it's only truncated
			if pg.Id == rootPgNo && flag != dropFlag {
				pg.Truncate()
				pg.Type = page.LeafPage // Now, the tree consists of the root only
				if err = btree.pgr.WritePage(pg); err != nil {
					return nil, 0, err
				}
				continue
			}

			if err = btree.pgr.FreePage(pg.Id); err != nil {
				return nil, 0, err
			}
		}
	}

	return leafCells, numRows, nil
}

func (btree *Btree) DumpBTree(tblName string, rootPgNo uint32) string {
	return btree.pgr.DumpTable(tblName, rootPgNo)
}

func (btree *Btree) DumpBTreePage(pgNo uint32) string {
	return btree.pgr.DumpPage(pgNo)
}

func (btree *Btree) Close() error {
	if btree.pgr != nil {
		return btree.pgr.Close()
	}
	return nil
}
