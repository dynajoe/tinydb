package pager

import (
	"fmt"
	"sync"

	"github.com/joeandaverde/tinydb/internal/storage"
)

type PageReader interface {
	Read(page int) (*MemPage, error)
}

type PageWriter interface {
	Write(pages ...*MemPage) error
	Allocate(PageType) (*MemPage, error)
	Flush() error
	Reset()
}

// Pager manages database paging
type Pager interface {
	PageReader
	PageWriter
}

type pager struct {
	mu *sync.RWMutex

	pageCount int
	pageCache map[int]*MemPage

	file storage.File
}

func Initialize(file storage.File) error {
	newPage := &MemPage{
		header:     NewPageHeader(PageTypeLeaf, file.PageSize()),
		pageNumber: 1,
		data:       make([]byte, file.PageSize()),
	}
	newPage.updateHeaderData()

	return file.Write(storage.Page{PageNumber: 1, Data: newPage.data})
}

func NewPager(file storage.File) Pager {
	return &pager{
		mu:        &sync.RWMutex{},
		pageCount: file.TotalPages(),
		pageCache: make(map[int]*MemPage),
		file:      file,
	}
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
	data, err := p.file.Read(pageNumber)
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
	for _, pg := range pages {
		p.pageCache[pg.Number()] = pg
	}

	return nil
}

// Flush flushes all dirty pages to destination
func (p *pager) Flush() error {
	var dirtyPages []storage.Page
	var dirtyMemPages []*MemPage
	for _, page := range p.pageCache {
		if !page.dirty {
			continue
		}

		dirtyPages = append(dirtyPages, storage.Page{PageNumber: page.pageNumber, Data: page.data})
		dirtyMemPages = append(dirtyMemPages, page)
	}

	if len(dirtyPages) > 0 {
		if err := p.file.Write(dirtyPages...); err != nil {
			return err
		}
		p.pageCount = p.file.TotalPages()
	}

	for _, p := range dirtyMemPages {
		p.dirty = false
	}

	return nil
}

// Reset clears all dirty pages
func (p *pager) Reset() {
	p.pageCount = p.file.TotalPages()
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
	p.pageCount = p.pageCount + 1
	newPage := &MemPage{
		header:     NewPageHeader(pageType, p.file.PageSize()),
		pageNumber: p.pageCount,
		data:       make([]byte, p.file.PageSize()),
		dirty:      true,
	}
	newPage.updateHeaderData()
	p.pageCache[p.pageCount] = newPage
	return p.pageCache[p.pageCount], nil
}

var _ Pager = (*pager)(nil)
