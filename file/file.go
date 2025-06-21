package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FileManager struct {
	dbName       string
	dbDirPath    string
	dbPageSize   int
	dbHeaderSize int
	dbFile       *os.File
}

func NewFileManager(dbFilePath string, dbPageSize int, dbHeaderSize int) (*FileManager, error) {
	fm := &FileManager{}

	fm.dbName = filepath.Base(dbFilePath)
	fm.dbDirPath = filepath.Dir(dbFilePath)
	fm.dbPageSize = dbPageSize
	fm.dbHeaderSize = dbHeaderSize

	file, err := os.OpenFile(dbFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	fm.dbFile = file

	// Initialize the DB header for new DBs
	fileInfo, err := fm.dbFile.Stat()
	if err != nil {
		return nil, err
	}
	fmt.Println(fileInfo.Size())
	if fileInfo.Size() == 0 {
		emptyHeader := make([]byte, fm.dbHeaderSize)
		if err = fm.Append(emptyHeader); err != nil {
			return nil, err
		}
	}

	return fm, nil
}

func (fm *FileManager) Read(offset int64, size int) ([]byte, error) {
	if fm.dbFile == nil {
		return nil, os.ErrInvalid
	}

	buf := make([]byte, size)
	_, err := fm.dbFile.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (fm *FileManager) Write(offset int64, data []byte) error {
	if fm.dbFile == nil {
		return os.ErrInvalid
	}

	_, err := fm.dbFile.WriteAt(data, offset)
	if err != nil {
		return err
	}
	return nil
}

func (fm *FileManager) Append(data []byte) error {
	if fm.dbFile == nil {
		return os.ErrInvalid
	}

	_, err := fm.dbFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	_, err = fm.dbFile.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func WriteStringToFile(filePath string, fileContent string) error {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err = file.WriteString(fileContent); err != nil {
		return err
	}

	return file.Sync() // Ensure contents are flushed to disk
}

func (fm *FileManager) Close() error {
	if fm.dbFile != nil {
		return fm.dbFile.Close()
	}
	return nil
}
