package storage

import (
	"fmt"
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
		if err := header.Write(file); err != nil {
			return nil, err
		}
		if err := file.Sync(); err != nil {
			return nil, err
		}
		return &Pager{
			fileHeader: header,
			file:       file,
			pageCache:  make(map[int]*MemPage),
			mu:         &sync.RWMutex{},
		}, nil
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
	if page == 1 {
		header.CellsOffset = 100
		header.FreeOffset = 100
	}

	return &MemPage{
		PageHeader: header,
		PageNumber: page,
		Data:       make([]byte, pageSize),
	}
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

	if _, err := p.file.Seek(PageOffset(page, p.fileHeader.PageSize), 0); err != nil {
		return nil, err
	}

	// Read the TablePage and cache the result
	tablePage, err := ReadPage(page, p.fileHeader.PageSize, p.file)
	if err != nil {
		return nil, err
	}
	p.pageCache[page] = tablePage

	return tablePage, nil
}

// Write writes a TablePage to disk at the specified page index
func (p *Pager) Write(page *MemPage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if page.PageNumber < 1 || page.PageNumber > p.pageCount {
		return fmt.Errorf("page [%d] out of bounds", page)
	}

	// Overwrite the entire page
	if _, err := p.file.Seek(PageOffset(page.PageNumber, page.PageSize), 0); err != nil {
		return err
	}

	// Write the page to disk and update cache
	if err := page.Write(p.file); err != nil {
		return err
	}

	// fsync
	if err := p.file.Sync(); err != nil {
		return err
	}

	p.pageCache[page.PageNumber] = page

	return nil
}

// Allocate virtually allocates a new page in the pager for a TablePage
func (p *Pager) Allocate() (*MemPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pageCount = p.pageCount + 1
	return NewPage(p.pageCount, p.fileHeader.PageSize), nil
}
