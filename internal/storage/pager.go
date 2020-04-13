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

// NewPager opens a new pager using the path specified
func NewPager(file *os.File) *Pager {
	if _, err := file.Seek(0, 0); err != nil {
		panic(err.Error())
	}

	header := ReadHeader(file)

	return &Pager{
		fileHeader: header,
		file:       file,
		pageCache:  make(map[int]*MemPage),
		mu:         &sync.RWMutex{},
	}
}

func NewPage(page int, pageSize uint16) *MemPage {
	dataLen := pageSize
	if page == 1 {
		dataLen = pageSize - 100
	}
	return &MemPage{
		PageHeader: NewPageHeader(PageTypeLeaf, dataLen),
		PageNumber: page,
		Data:       make([]byte, dataLen),
	}
}

// Read reads a TablePage from disk at the specified page index
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

	// Read raw bytes into a TablePage starting at offset
	if _, err := p.file.Seek(p.pageOffset(page), 0); err != nil {
		return nil, err
	}

	// Read the TablePage and cache the result
	tablePage, err := ReadPage(p.file, p.fileHeader.PageSize)
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

	if _, err := p.file.Seek(p.pageOffset(page.PageNumber), 0); err != nil {
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
func (p *Pager) Allocate() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pageCount = p.pageCount + 1
	return nil
}

func (p *Pager) pageOffset(page int) int64 {
	// Page 1 starts after the file header (100 bytes)
	if page == 1 {
		return 100
	}
	return int64(page-1) * int64(p.fileHeader.PageSize)
}
