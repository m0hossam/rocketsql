package storage

import (
	"errors"
	"fmt"
	"os"
	"syscall"
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

	off := int64(ptr-1) * int64(dbPageSize) // offset to write at (pages are numbered from 1)
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

func loadJournalFromDisk(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func flushFile(path string, b []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_SYNC, os.ModeExclusive) // O_SYNC guarantees flushing?
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(b)
	if err != nil {
		return err
	}

	handle := syscall.Handle(file.Fd())
	return syscall.FlushFileBuffers(handle) // using this instead of fsync to guarantee flushing?
}

func deleteFile(path string) error {
	return os.Remove(path)
}

func truncateFile(path string, extraPages int) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}

	extraSize := extraPages * dbPageSize

	newSize := fi.Size() - int64(extraSize)
	if newSize < 0 {
		newSize = 0 // Prevent negative file sizes
	}

	// Truncate the file to the new size
	return os.Truncate(path, newSize)
}

func getDbNumPages(path string) (uint32, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return DbNullPage, err
	}

	return uint32(fi.Size() / dbPageSize), nil
}

func GetFirstFreePagePtr(path string) (*uint32, error) {
	dbNumPages, err := getDbNumPages(path)
	if err != nil {
		return nil, err
	}
	var firstFreePgPtr uint32 = uint32(dbNumPages + 1)
	return &firstFreePgPtr, nil
}
