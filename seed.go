package main

import (
	"encoding/binary"
	"fmt"
)

func fillUpPage9() error {
	err := openDB("db.rocketsql")
	if err != nil {
		err = createAndSeedDB()
		if err != nil {
			return err
		}
	}

	pg1, err := loadPageOne()
	if err != nil {
		return err
	}
	pg2, err := loadPage(2)
	if err != nil {
		return err
	}

	for i := 0; i < 405; i++ { // TODO: Remove this
		val := 26 + i
		key := uint32(val)
		payload := make([]byte, 2)
		binary.Encode(payload, binary.BigEndian, uint16(val))
		err = insert(key, payload, pg2, &pg1.firstFreePtr)
		if err != nil {
			return err
		}
	}

	return nil
}

func createAndSeedDB() error {
	err := createDB("db.rocketsql")
	if err != nil {
		return fmt.Errorf("failed to create DB: %s", err)
	}

	p1, err := loadPageOne()
	if err != nil {
		return fmt.Errorf("failed to open page one: %s", err)
	}

	p2, err := createPage(interiorPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Errorf("failed to open page: %s", err)
	}
	p2.nCells = 1
	p2.lastPtr = 4
	off := dbPageSize - sizeofCellKey - sizeofCellPtr
	p2.cellOffArr = append(p2.cellOffArr, off)
	c := cell{
		key: 13,
		ptr: 3,
	}
	p2.cells[off] = c
	p2.nFreeBytes = dbPageSize - dbPageHdrSize - p2.nCells*sizeofCellOff - p2.nCells*8
	err = saveNewPage(p2)
	if err != nil {
		return fmt.Errorf("failed to save page to disk: %s", err)
	}

	p3, err := createPage(interiorPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Errorf("failed to open page: %s", err)
	}
	p3.nCells = 2
	p3.lastPtr = 7
	off = dbPageSize - sizeofCellKey - sizeofCellPtr
	p3.cellOffArr = append(p3.cellOffArr, off)
	c = cell{
		key: 9,
		ptr: 5,
	}
	p3.cells[off] = c
	off = dbPageSize - 2*sizeofCellKey - 2*sizeofCellPtr
	p3.cellOffArr = append(p3.cellOffArr, off)
	c = cell{
		key: 11,
		ptr: 6,
	}
	p3.cells[off] = c
	p3.nFreeBytes = dbPageSize - dbPageHdrSize - p3.nCells*sizeofCellOff - p3.nCells*8
	err = saveNewPage(p3)
	if err != nil {
		return fmt.Errorf("failed to save page to disk: %s", err)
	}

	p4, err := createPage(interiorPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Errorf("failed to open page: %s", err)
	}
	p4.nCells = 1
	p4.lastPtr = 9
	off = dbPageSize - sizeofCellKey - sizeofCellPtr
	p4.cellOffArr = append(p4.cellOffArr, off)
	c = cell{
		key: 16,
		ptr: 8,
	}
	p4.cells[off] = c
	p4.nFreeBytes = dbPageSize - dbPageHdrSize - p4.nCells*sizeofCellOff - p4.nCells*8
	err = saveNewPage(p4)
	if err != nil {
		return fmt.Errorf("failed to save page to disk: %s", err)
	}

	pyldSz := uint16(2)
	cellSz := uint16(sizeofCellKey + sizeofCellPayloadSize + pyldSz)

	p5, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Errorf("failed to open page: %s", err)
	}
	p5.nCells = 2
	p5.lastPtr = 6
	off = dbPageSize - cellSz
	buf := make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(1))
	c = cell{
		key:         1,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p5.cellOffArr = append(p5.cellOffArr, off)
	p5.cells[off] = c
	off = dbPageSize - 2*cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(4))
	c = cell{
		key:         4,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p5.cellOffArr = append(p5.cellOffArr, off)
	p5.cells[off] = c
	p5.nFreeBytes = dbPageSize - dbPageHdrSize - p5.nCells*sizeofCellOff - p5.nCells*cellSz
	err = saveNewPage(p5)
	if err != nil {
		return fmt.Errorf("failed to save page to disk: %s", err)
	}

	p6, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Errorf("failed to open page: %s", err)
	}
	p6.nCells = 2
	p6.lastPtr = 7
	off = dbPageSize - cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(9))
	c = cell{
		key:         9,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p6.cellOffArr = append(p6.cellOffArr, off)
	p6.cells[off] = c
	off = dbPageSize - 2*cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(10))
	c = cell{
		key:         10,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p6.cellOffArr = append(p6.cellOffArr, off)
	p6.cells[off] = c
	p6.nFreeBytes = dbPageSize - dbPageHdrSize - p6.nCells*sizeofCellOff - p6.nCells*cellSz
	err = saveNewPage(p6)
	if err != nil {
		return fmt.Errorf("failed to save page to disk: %s", err)
	}

	p7, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Errorf("failed to open page: %s", err)
	}
	p7.nCells = 2
	p7.lastPtr = 8
	off = dbPageSize - cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(11))
	c = cell{
		key:         11,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p7.cellOffArr = append(p7.cellOffArr, off)
	p7.cells[off] = c
	off = dbPageSize - 2*cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(12))
	c = cell{
		key:         12,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p7.cellOffArr = append(p7.cellOffArr, off)
	p7.cells[off] = c
	p7.nFreeBytes = dbPageSize - dbPageHdrSize - p7.nCells*sizeofCellOff - p7.nCells*cellSz
	err = saveNewPage(p7)
	if err != nil {
		return fmt.Errorf("failed to save page to disk: %s", err)
	}

	p8, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Errorf("failed to open page: %s", err)
	}
	p8.nCells = 2
	p8.lastPtr = 9
	off = dbPageSize - cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(13))
	c = cell{
		key:         13,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p8.cellOffArr = append(p8.cellOffArr, off)
	p8.cells[off] = c
	off = dbPageSize - 2*cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(15))
	c = cell{
		key:         15,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p8.cellOffArr = append(p8.cellOffArr, off)
	p8.cells[off] = c
	p8.nFreeBytes = dbPageSize - dbPageHdrSize - p8.nCells*sizeofCellOff - p8.nCells*cellSz
	err = saveNewPage(p8)
	if err != nil {
		return fmt.Errorf("failed to save page to disk: %s", err)
	}

	p9, err := createPage(leafPage, &p1.firstFreePtr)
	if err != nil {
		return fmt.Errorf("failed to open page: %s", err)
	}
	p9.nCells = 3
	p9.lastPtr = dbNullPage
	off = dbPageSize - cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(16))
	c = cell{
		key:         16,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p9.cellOffArr = append(p9.cellOffArr, off)
	p9.cells[off] = c
	off = dbPageSize - 2*cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(20))
	c = cell{
		key:         20,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p9.cellOffArr = append(p9.cellOffArr, off)
	p9.cells[off] = c
	off = dbPageSize - 3*cellSz
	buf = make([]byte, pyldSz)
	binary.Encode(buf, binary.BigEndian, uint16(25))
	c = cell{
		key:         25,
		payloadSize: pyldSz,
		payload:     buf,
	}
	p9.cellOffArr = append(p9.cellOffArr, off)
	p9.cells[off] = c
	p9.nFreeBytes = dbPageSize - dbPageHdrSize - p9.nCells*sizeofCellOff - p9.nCells*cellSz
	err = saveNewPage(p9)
	if err != nil {
		return fmt.Errorf("failed to save page to disk: %s", err)
	}

	err = savePageOne(p1)
	if err != nil {
		return fmt.Errorf("failed to save page one to disk: %s", err)
	}

	return nil
}
