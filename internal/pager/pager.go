package pager

import (
	"errors"
	"fmt"
	"sync"

	"github.com/joeandaverde/tinydb/internal/storage"
)

type Mode int

const (
	ModeNone Mode = iota
	ModeRead
	ModeWrite
)

// Pager manages database paging
type Pager interface {
	Mode() Mode
	SetMode(Mode)
	PageSize() int
	Read(page int) (*MemPage, error)
	Write(pages ...*MemPage) error
	Allocate(PageType) (*MemPage, error)
	Flush() error
	Reset()
}

type pager struct {
	mu     *sync.RWMutex
	notify sync.Cond
	mode   Mode

	pageCount int
	pageSize  int
	pageCache map[int]*MemPage

	src storage.PageReader
	dst storage.PageWriter
}

func NewPager(src storage.PageReader, dst storage.PageWriter) Pager {
	return &pager{
		mu:        &sync.RWMutex{},
		mode:      ModeRead,
		pageCount: src.TotalPages(),
		pageSize:  src.PageSize(),
		pageCache: make(map[int]*MemPage),
		src:       src,
		dst:       dst,
	}
}

// PageSize returns the page size of the pager
func (p *pager) PageSize() int {
	return p.pageSize
}

// SetMode sets the mode of the pager to assist with protecting against unexpected modification
func (p *pager) SetMode(mode Mode) {
	p.mode = mode
}

// Mode returns the current state of the pager
func (p *pager) Mode() Mode {
	return p.mode
}

// Read reads a full page from cache or the page source
func (p *pager) Read(pageNumber int) (*MemPage, error) {
	if pageNumber < 1 {
		return nil, fmt.Errorf("page [%d] out of bounds", pageNumber)
	}

	if tablePage, ok := p.pageCache[pageNumber]; ok {
		return tablePage, nil
	}

	// Ensure the page hasn't been retrieved into the cache since releasing the read lock
	if tablePage, ok := p.pageCache[pageNumber]; ok {
		return tablePage, nil
	}

	// Read raw page data from the source
	data, err := p.src.Read(pageNumber)
	if err != nil {
		return nil, err
	}

	// Parse bytes to a page
	page, err := FromBytes(pageNumber, data)
	if err != nil {
		return nil, err
	}

	// Cache the result for later reads
	p.pageCache[pageNumber] = page

	return p.pageCache[pageNumber], nil
}

// Write updates pages in the pager
func (p *pager) Write(pages ...*MemPage) error {
	if p.mode != ModeWrite {
		return errors.New("write: cannot modify pager in read state")
	}

	for _, pg := range pages {
		p.pageCache[pg.Number()] = pg
	}

	return nil
}

// Flush flushes all dirty pages to destination
func (p *pager) Flush() error {
	if p.mode != ModeWrite {
		return errors.New("flush: cannot modify pager in read state")
	}

	for _, page := range p.pageCache {
		if !page.dirty {
			continue
		}

		if err := p.dst.Write(page.pageNumber, page.data); err != nil {
			return err
		}

		page.dirty = false
	}

	p.pageCount = p.src.TotalPages()

	return nil
}

// Reset clears all dirty pages
func (p *pager) Reset() {
	p.pageCount = p.src.TotalPages()
	for k, page := range p.pageCache {
		if page.dirty {
			delete(p.pageCache, k)
		}
	}
}

// Allocate allocates a new dirty page in the pager.
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
func (p *pager) Allocate(pageType PageType) (*MemPage, error) {
	if p.mode != ModeWrite {
		return nil, errors.New("allocate: cannot modify pager in read state")
	}

	p.pageCount = p.pageCount + 1
	newPage := &MemPage{
		header:     NewPageHeader(pageType, p.pageSize),
		pageNumber: p.pageCount,
		data:       make([]byte, p.pageSize),
		dirty:      true,
	}
	newPage.updateHeaderData()
	p.pageCache[p.pageCount] = newPage
	return p.pageCache[p.pageCount], nil
}

var _ Pager = (*pager)(nil)
