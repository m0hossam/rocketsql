package storage

type Iterator struct {
	pgr *Pager
	p   *page
	i   int
}

func createIterator(pgr *Pager, pg *page, idx int) *Iterator {
	it := &Iterator{
		pgr: pgr,
		p:   pg,
		i:   idx,
	}
	return it
}

func (it *Iterator) Next() (string, bool, error) { // returns row, isNotEnd
	for it.i >= int(it.p.nCells) {
		if it.p.lastPtr == DbNullPage {
			return "", false, nil
		}

		pg, err := it.pgr.LoadPage(it.p.lastPtr)
		if err != nil {
			return "", true, err
		}

		it.p = pg
		it.i = 0
	}
	b := it.p.cells[it.p.cellPtrArr[it.i]].value
	row := DeserializeRow(b)
	it.i++
	return row, true, nil
}
