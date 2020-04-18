package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// Pager manages database paging to and from disk
type Pager struct {
	fileHeader FileHeader
	file       *os.File
	pageCount  int
	pageCache  map[int]*MemPage
	mu         *sync.RWMutex
}

type CursorType byte

const (
	CURSOR_UNKNOWN = 0
	CURSOR_READ    = 1
	CURSOR_WRITE   = 2
)

type Cursor struct {
	typ   CursorType
	file  *os.File
	start int64
}

func (c *Cursor) Rewind() error {
	_, err := c.file.Seek(c.start, 0)
	return err
}

// Open opens a new pager using the path specified.
// The pager owns the file.
func Open(path string) (*Pager, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Set up the file header for the new database
	if info.Size() == 0 {
		header := NewFileHeader()
		pager := &Pager{
			fileHeader: header,
			file:       file,
			pageCount:  0,
			pageCache:  make(map[int]*MemPage),
			mu:         &sync.RWMutex{},
		}

		// Persist the header
		if _, err := header.WriteTo(pager.file); err != nil {
			return nil, err
		}

		// Allocate and then persist the first page
		pageOne, err := pager.Allocate()
		if err != nil {
			return nil, err
		}
		if err := pager.Write(pageOne); err != nil {
			return nil, err
		}

		return pager, nil
	}

	// Opening an existing database
	headerBytes := make([]byte, 100)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		return nil, err
	}

	header := ParseFileHeader(headerBytes)
	return &Pager{
		fileHeader: header,
		file:       file,
		pageCount:  int(info.Size()) / int(header.PageSize),
		pageCache:  make(map[int]*MemPage),
		mu:         &sync.RWMutex{},
	}, nil
}

// NewPage creates a new database page.
//
// Page 1 of a database file is the root page of a table b-tree that
// holds a special table named "sqlite_master" (or "sqlite_temp_master" in
// the case of a TEMP database) which stores the complete database schema.
// The structure of the sqlite_master table is as if it had been
// created using the following SQL:
//
// CREATE TABLE sqlite_master(
//    type text,
//    name text,
//    tbl_name text,
//    rootpage integer,
//    sql text
// );
func NewPage(page int, pageSize uint16) *MemPage {
	header := NewPageHeader(PageTypeLeaf, pageSize)
	return &MemPage{
		PageHeader: header,
		PageNumber: page,
		Data:       make([]byte, pageSize),
	}
}

func (p *Pager) OpenRead(page int) (*Cursor, error) {
	return p.open(page, CURSOR_READ)
}

func (p *Pager) OpenWrite(page int) (*Cursor, error) {
	return p.open(page, CURSOR_WRITE)
}

func (p *Pager) open(page int, typ CursorType) (*Cursor, error) {
	mode := os.O_RDONLY
	if typ == CURSOR_WRITE {
		mode = os.O_RDWR
	}

	// TODO: add usercookie or something to lock this page if necessary
	f, err := os.OpenFile(p.file.Name(), mode, os.ModePerm)
	if err != nil {
		return nil, err
	}
	start := p.pageOffset(page)
	if _, err := f.Seek(start, 0); err != nil {
		return nil, err
	}
	return &Cursor{
		typ:   typ,
		file:  f,
		start: start,
	}, nil
}

func (p *Pager) CloseCursor(c *Cursor) {
	_ = c.file.Close()
}

// Read reads a full page from disk
func (p *Pager) Read(page int) (*MemPage, error) {
	p.mu.RLock()
	if page < 1 || page > p.pageCount {
		return nil, fmt.Errorf("page [%d] out of bounds", page)
	}

	if tablePage, ok := p.pageCache[page]; ok {
		p.mu.RUnlock()
		return tablePage, nil
	}

	// Release the read lock, in order to upgrade to a write lock
	p.mu.RUnlock()

	// Upgrade the lock to writer because we change the underlying
	// file offset and update cache.
	p.mu.Lock()
	defer p.mu.Unlock()

	// Ensure the page hasn't been retrieved into the cache since releasing the read lock
	// TODO: probably should verify the page count again
	// It may be better to actually capture a cache signature/version with the read lock, that way we can
	// potentially fail if a page has been replaced.
	if tablePage, ok := p.pageCache[page]; ok {
		return tablePage, nil
	}

	if _, err := p.file.Seek(p.pageOffset(page), 0); err != nil {
		return nil, err
	}

	// Read the TablePage and cache the result
	tablePage, err := readPage(page, p.fileHeader.PageSize, p.file)
	if err != nil {
		return nil, err
	}
	p.pageCache[page] = tablePage

	return tablePage, nil
}

// WriteTo writes all pages to disk
func (p *Pager) Write(pages ...*MemPage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, page := range pages {
		if page.PageNumber < 1 || page.PageNumber > p.pageCount {
			return fmt.Errorf("page [%d] out of bounds", page)
		}

		// Overwrite the entire page
		offset := p.pageOffset(page.PageNumber)
		if _, err := p.file.Seek(offset, 0); err != nil {
			return err
		}

		// WriteTo the page to disk and update cache
		if err := page.Write(p.file); err != nil {
			return err
		}

		p.pageCache[page.PageNumber] = page
	}

	// Update file header
	if err := p.updateFileHeader(); err != nil {
		return err
	}

	// fsync
	if err := p.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (p *Pager) updateFileHeader() error {
	p.fileHeader.FileChangeCounter = p.fileHeader.FileChangeCounter + 1
	p.fileHeader.SizeInPages = uint32(p.pageCount)

	fileHeaderBuf := bytes.Buffer{}
	if _, err := p.fileHeader.WriteTo(&fileHeaderBuf); err != nil {
		return err
	}

	// Write the file header to disk
	if _, err := p.file.WriteAt(fileHeaderBuf.Bytes(), 0); err != nil {
		return err
	}
	return nil
}

// Allocate virtually allocates a new page in the pager for a TablePage
func (p *Pager) Allocate() (*MemPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pageCount = p.pageCount + 1
	page := NewPage(p.pageCount, p.fileHeader.PageSize)
	return page, nil
}

func readPage(page int, pageSize uint16, reader io.Reader) (*MemPage, error) {
	if page == 1 {
		pageSize = pageSize - 100
	}
	data := make([]byte, pageSize)

	bytesRead, err := reader.Read(data)
	if err != nil {
		return nil, err
	}
	if bytesRead != int(pageSize) {
		return nil, errors.New("unexpected page size")
	}

	header := PageHeader{
		Type:                PageType(data[0]),
		FreeBlock:           binary.BigEndian.Uint16(data[1:3]),
		NumCells:            binary.BigEndian.Uint16(data[3:5]),
		CellsOffset:         binary.BigEndian.Uint16(data[5:7]),
		FragmentedFreeBytes: data[7],
		RightPage:           0,
	}
	if header.Type == PageTypeInternal || header.Type == PageTypeInternalIndex {
		header.RightPage = binary.BigEndian.Uint32(data[8:11])
	}

	return &MemPage{
		PageHeader: header,
		PageNumber: page,
		Data:       data,
	}, nil
}

func (p *Pager) pageOffset(page int) int64 {
	if page == 1 {
		return 100
	}
	return int64(page-1) * int64(p.fileHeader.PageSize)
}
