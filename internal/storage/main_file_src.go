package storage

import (
	"errors"
	"io"
	"os"
	"sync"
)

type File interface {
	PageReader
	PageWriter
}

type DbFile struct {
	path       string
	header     FileHeader
	file       *os.File
	pageSize   int
	totalPages int

	mu *sync.RWMutex
}

func OpenDbFile(path string, pageSize int) (*DbFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Opening an existing database
	var header FileHeader
	if info.Size() > 0 {
		headerBytes := make([]byte, 100)
		if _, err := file.ReadAt(headerBytes, 0); err != nil {
			return nil, err
		}

		header, err = ParseFileHeader(headerBytes)
		if err != nil {
			return nil, err
		}

		pageSize = int(header.PageSize)
	}

	return &DbFile{
		pageSize: pageSize,
		header:   header,
		file:     file,
		path:     path,
		// TODO: Should this be calculated from the file size?
		totalPages: int(header.SizeInPages),
		mu:         &sync.RWMutex{},
	}, nil
}

func (f *DbFile) Path() string {
	return f.path
}

func (f *DbFile) PageSize() int {
	return f.pageSize
}

func (f *DbFile) TotalPages() int {
	return f.totalPages
}

func (f *DbFile) Read(page int) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	offset := f.pageOffset(page)
	if _, err := f.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	readOffset := 0
	if page == 1 {
		readOffset = 100
	}

	data := make([]byte, f.pageSize)
	_, err := f.file.Read(data[readOffset:])
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (f *DbFile) Write(pages ...Page) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, page := range pages {
		if page.PageNumber > f.totalPages+1 {
			return errors.New("cannot grow the db file with a gap in pages")
		}

		if page.PageNumber > f.totalPages {
			f.totalPages = page.PageNumber
		}

		pageOffset := f.pageOffset(page.PageNumber)
		if _, err := f.file.Seek(pageOffset, io.SeekStart); err != nil {
			return err
		}

		readOffset := 0
		if page.PageNumber == 1 {
			readOffset = 100
		}

		if _, err := f.file.Write(page.Data[readOffset:]); err != nil {
			return err
		}
	}

	if err := f.updateFileHeader(); err != nil {
		return err
	}

	if err := f.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (f *DbFile) pageOffset(page int) int64 {
	if page == 1 {
		return 100
	}
	return int64(page-1) * int64(f.pageSize)
}

func (f *DbFile) updateFileHeader() error {
	f.header.FileChangeCounter = f.header.FileChangeCounter + 1
	f.header.SizeInPages = uint32(f.totalPages)
	f.header.PageSize = uint16(f.pageSize)

	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if _, err := f.header.WriteTo(f.file); err != nil {
		return err
	}

	return nil
}

var _ File = (*DbFile)(nil)
