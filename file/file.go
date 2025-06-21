package file

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

type FileManager struct {
	dbName     string
	dbDirPath  string
	dbPageSize int
	dbFile     *os.File
}

func NewFileManager(dbFilePath string, dbPageSize int) (*FileManager, error) {
	fm := &FileManager{}

	fm.dbName = filepath.Base(dbFilePath)
	fm.dbDirPath = filepath.Dir(dbFilePath)
	fm.dbPageSize = dbPageSize

	file, err := os.OpenFile(dbFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	fm.dbFile = file

	return fm, nil
}

func (fm *FileManager) Close() error {
	if fm.dbFile != nil {
		return fm.dbFile.Close()
	}
	return nil
}

func (fm *FileManager) Read(offset int64) ([]byte, error) {
	if fm.dbFile == nil {
		return nil, os.ErrInvalid
	}

	buf := make([]byte, fm.dbPageSize)
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

	if len(data) != fm.dbPageSize {
		return errors.New("byte array size does not match database page size")
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

	if len(data) != fm.dbPageSize {
		return errors.New("byte array size does not match database page size")
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

func (fm *FileManager) GetNumberOfPages() (int64, error) {
	if fm.dbFile == nil {
		return 0, os.ErrInvalid
	}

	fileInfo, err := fm.dbFile.Stat()
	if err != nil {
		return 0, err
	}

	size := fileInfo.Size()
	numPages := size / int64(fm.dbPageSize)
	return numPages, nil
}

func (fm *FileManager) WriteTo(filePath string, fileContent string) error {
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
