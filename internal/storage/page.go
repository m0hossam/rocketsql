package storage

var (
	DbMaxCellsPerPage uint16 = 3 // for testing purposes
)

const ( // database constants
	dbPageSize                = 512 // default is 4096 but can be any power of two between 512 and 65536
	dbPageHdrSize             = 12
	dbMinCellsPerPage         = 2 // should be atleat 2 to avoid insertion corner cases
	dbMaxCellSize             = (dbPageSize - dbPageHdrSize - dbMinCellsPerPage*sizeofCellOff) / dbMinCellsPerPage
	dbMinFreeBlockSize        = 4
	DbNullPage         uint32 = 0
)

const ( // page types
	InteriorPage = iota
	LeafPage
)

const ( // page constants
	offsetofPageType     = 0
	offsetofFreeListOff  = 1
	offsetofNumCells     = 3
	offsetofCellArrOff   = 5
	offsetofNumFragBytes = 7
	offsetofLastPtr      = 8
	offsetofCellPtrArr   = 12
	sizeofPageType       = 1
	sizeofFreeListOff    = 2
	sizeofNumCells       = 2
	sizeofCellArrOff     = 2
	sizeofNumFragBytes   = 1
	sizeofLastPtr        = 4
	sizeofCellOff        = 2
)

const ( // free block constants
	offsetofFreeBlockSize    = 0
	offsetofFreeBlockNextOff = 2
	sizeofFreeBlockSize      = 2
	sizeofFreeBlockNextOff   = 2
	sizeofFreeBlockHdr       = sizeofFreeBlockNextOff + sizeofFreeBlockSize
)

const ( // cell constants
	sizeofCellKeySize   = 2
	sizeofCellValueSize = 2
)

type freeBlock struct {
	offset uint16 // not serialized
	size   uint16
	next   *freeBlock
}

type cell struct {
	key   []byte
	value []byte
}

type page struct {
	id         uint32
	pType      uint8
	freeList   *freeBlock
	nCells     uint16
	cellArrOff uint16
	nFragBytes uint8
	lastPtr    uint32
	cellPtrArr []uint16
	cells      map[uint16]cell
}

func truncatePage(p *page) {
	p.nCells = 0
	p.cellArrOff = dbPageSize
	p.nFragBytes = 0
	p.lastPtr = DbNullPage
	p.cellPtrArr = []uint16{}
	p.cells = map[uint16]cell{}
}

func copyPage(dst *page, src *page) {
	dst.pType = src.pType
	dst.freeList = &freeBlock{}
	if src.freeList != nil {
		dst.freeList = &freeBlock{}
		*dst.freeList = *src.freeList
	} else {
		dst.freeList = nil
	}
	dst.nCells = src.nCells
	dst.cellArrOff = src.cellArrOff
	dst.nFragBytes = src.nFragBytes
	dst.lastPtr = src.lastPtr
	dst.cellPtrArr = make([]uint16, len(src.cellPtrArr))
	copy(dst.cellPtrArr, src.cellPtrArr)
	dst.cells = map[uint16]cell{}
	for k, v := range src.cells {
		dst.cells[k] = v
	}
}
