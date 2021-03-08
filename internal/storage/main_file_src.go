package storage

import (
	"errors"
	"io"
	"os"
	"sync"
)

type DbFile struct {
	mu         *sync.RWMutex
	header     FileHeader
	file       *os.File
	path       string
	pageSize   int
	totalPages int
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

		header = ParseFileHeader(headerBytes)
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

func (s *DbFile) PageSize() int {
	return s.pageSize
}

func (s *DbFile) TotalPages() int {
	return s.totalPages
}

func (s *DbFile) Read(page int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offset := s.pageOffset(page)
	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	readOffset := 0
	if page == 1 {
		readOffset = 100
	}
	data := make([]byte, s.pageSize)
	_, err := s.file.Read(data[readOffset:])
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *DbFile) Write(page int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if page > s.totalPages+1 {
		return errors.New("cannot grow the db file with a gap in pages")
	}

	if page > s.totalPages {
		s.totalPages = page
	}

	pageOffset := s.pageOffset(page)
	if _, err := s.file.Seek(pageOffset, io.SeekStart); err != nil {
		return err
	}

	readOffset := 0
	if page == 1 {
		readOffset = 100
	}
	if _, err := s.file.Write(data[readOffset:]); err != nil {
		return err
	} else if err := s.updateFileHeader(); err != nil {
		return err
	} else if err := s.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (s *DbFile) pageOffset(page int) int64 {
	if page == 1 {
		return 100
	}
	return int64(page-1) * int64(s.pageSize)
}

func (s *DbFile) updateFileHeader() error {
	s.header.FileChangeCounter = s.header.FileChangeCounter + 1
	s.header.SizeInPages = uint32(s.totalPages)
	s.header.PageSize = uint16(s.pageSize)

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	} else if _, err := s.header.WriteTo(s.file); err != nil {
		return err
	}

	return nil
}

var _ PageReader = (*DbFile)(nil)
var _ PageWriter = (*DbFile)(nil)
