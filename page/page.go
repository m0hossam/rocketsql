package page

import (
	"errors"
	"maps"
)

var (
	MaxCellsPerPage uint16 = 3 // for testing purposes (TODO: REMOVE THIS)
)

const ( // database header constants
	DbHeaderSize           = 12
	OffsetOfDbNumPages     = 0
	OffsetOfDbFreePage     = 4
	OffsetOfDbNumFreePages = 8
	SizeOfDbNumPages       = 4
	SizeOfDbFreePage       = 4
	SizeOfDbNumFreePages   = 4
)

const ( // database constants
	DefaultPageSize         = 512 // default is 4096 but can be any power of two between 512 and 65536 (TODO: CHANGE THIS)
	PageHeaderSize          = 12
	MinCellsPerPage         = 2 // should be atleat 2 to avoid insertion corner cases
	MaxCellSize             = (DefaultPageSize - PageHeaderSize - MinCellsPerPage*SizeOfCellOff) / MinCellsPerPage
	MinFreeBlockSize        = 4
	DbNullPage       uint32 = 0
)

const ( // page types
	InteriorPage uint8 = 0
	LeafPage     uint8 = 1
)

const ( // page constants
	OffsetOfPageType     = 0
	OffsetOfFreeListOff  = 1
	OffsetOfNumCells     = 3
	OffsetOfCellArrOff   = 5
	OffsetOfNumFragBytes = 7
	OffsetOfLastPtr      = 8
	OffsetOfCellPtrArr   = 12
	SizeOfPageType       = 1
	SizeOfFreeListOff    = 2
	SizeOfNumCells       = 2
	SizeOfCellArrOff     = 2
	SizeOfNumFragBytes   = 1
	SizeOfLastPtr        = 4
	SizeOfCellOff        = 2
)

const ( // free block constants
	OffsetOfFreeBlockSize    = 0
	OffsetOfFreeBlockNextOff = 2
	SizeOfFreeBlockSize      = 2
	SizeOfFreeBlockNextOff   = 2
	SizeOfFreeBlockHdr       = SizeOfFreeBlockNextOff + SizeOfFreeBlockSize
)

const ( // cell constants
	SizeOfCellKeySize   = 2
	SizeOfCellValueSize = 2
)

type FreeBlock struct {
	Offset uint16 // not serialized
	Size   uint16
	Next   *FreeBlock
}

type Cell struct {
	Key   []byte
	Value []byte
}

type DbHeader struct {
	NumPages      uint32
	FirstFreePage uint32
	NumFreePages  uint32
}

type Page struct {
	Id           uint32
	Type         uint8
	FreeList     *FreeBlock
	NumCells     uint16
	CellArrOff   uint16
	NumFragBytes uint8
	LastPtr      uint32
	CellPtrArr   []uint16
	Cells        map[uint16]Cell
}

func NewPage(pType uint8, newPtr *uint32) (*Page, error) {
	if pType != InteriorPage && pType != LeafPage { // invalid type
		return nil, errors.New("invalid page type")
	}

	pg := &Page{
		Id:           *newPtr,
		Type:         pType,
		NumCells:     0,
		CellArrOff:   DefaultPageSize,
		NumFragBytes: 0,
		LastPtr:      DbNullPage,
		CellPtrArr:   []uint16{},
		Cells:        map[uint16]Cell{},
	}
	*newPtr++
	return pg, nil
}

func (pg *Page) Truncate() {
	pg.FreeList = nil
	pg.NumCells = 0
	pg.CellArrOff = DefaultPageSize
	pg.NumFragBytes = 0
	pg.LastPtr = DbNullPage
	pg.CellPtrArr = []uint16{}
	pg.Cells = map[uint16]Cell{}
}

func (src *Page) CopyTo(dst *Page) {
	dst.Type = src.Type
	dst.FreeList = &FreeBlock{}
	if src.FreeList != nil {
		dst.FreeList = &FreeBlock{}
		*dst.FreeList = *src.FreeList
	} else {
		dst.FreeList = nil
	}
	dst.NumCells = src.NumCells
	dst.CellArrOff = src.CellArrOff
	dst.NumFragBytes = src.NumFragBytes
	dst.LastPtr = src.LastPtr
	dst.CellPtrArr = make([]uint16, len(src.CellPtrArr))
	copy(dst.CellPtrArr, src.CellPtrArr)
	dst.Cells = map[uint16]Cell{}
	maps.Copy(dst.Cells, src.Cells)
}
