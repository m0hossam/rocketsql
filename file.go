package main

import (
	"errors"
	"fmt"
	"os"
)

// TODO: Research fsync and I/O race

func openFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	f.Close()
	return nil
}

func createFile(path string) error {
	f, err := os.Create(path) // creates new file or truncates exiting file (deletes everything in it)
	if err != nil {
		return err
	}
	f.Close()
	return nil
}

func saveNewPageToDisk(path string, data []byte) error {
	if len(data) != int(dbPageSize) {
		return fmt.Errorf("length of byte sequence (%d) must be equal to a page size (%d)", len(data), dbPageSize)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, os.ModeExclusive)
	if err != nil {
		return err
	}
	defer f.Close() // file is now open, always close it after function returns

	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func savePageToDisk(path string, data []byte, ptr uint32) error {
	if len(data) != int(dbPageSize) {
		return errors.New("length of byte sequence must be equal to a page size")
	}

	f, err := os.OpenFile(path, os.O_WRONLY, os.ModeExclusive)
	if err != nil {
		return err
	}
	defer f.Close() // file is now open, always close it after function returns

	off := int64((ptr - 1) * uint32(dbPageSize)) // offset to write at (pages are numbered from 1)
	_, err = f.WriteAt(data, off)
	if err != nil {
		return err
	}
	return nil
}

/*
Reads page numbered (ptr)
and returns its serialized form
*/
func loadPageFromDisk(path string, ptr uint32) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() // file is now open, always close it after function returns

	b := make([]byte, dbPageSize)                // allocate a [dbPageSize]byte slice
	off := int64((ptr - 1) * uint32(dbPageSize)) // offset to read from (pages are numbered from 1)
	_, err = f.ReadAt(b, off)
	if err != nil {
		return nil, err
	}

	return b, err
}
