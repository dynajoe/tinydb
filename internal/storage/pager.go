package storage

import (
	"errors"
	"fmt"
	"sync"
)

type Mode int

const (
	ModeRead Mode = iota
	ModeWrite
)

type State int

const (
	StateRead State = iota
	StateWrite
)

// Pager manages database paging
type Pager interface {
	State() State
	PageSize() int
	Read(page int) (*MemPage, error)
	Write(pages ...*MemPage) error
	Allocate(PageType) (*MemPage, error)
	Flush() error
	Reset()
}

type ReservedPager struct {
	releaseOnce *sync.Once
	upgradeOnce *sync.Once
	pager       Pager

	Release func()
	Upgrade func()
}

type ReservablePager interface {
	Reserve(Mode) *ReservedPager
}

type PageReader interface {
	PageSize() int
	TotalPages() int
	Read(page int) ([]byte, error)
}

type PageWriter interface {
	Write(page int, data []byte) error
}
type pager struct {
	mu     *sync.RWMutex
	notify sync.Cond
	state  State

	pageCount     int
	pageSize      int
	pageCache     map[int]*MemPage
	modifiedPages map[int]*MemPage

	src PageReader
	dst PageWriter
}

func NewReservablePager(src PageReader, dst PageWriter) ReservablePager {
	return &pager{
		mu:            &sync.RWMutex{},
		state:         StateRead,
		pageCount:     src.TotalPages(),
		pageSize:      src.PageSize(),
		pageCache:     make(map[int]*MemPage),
		modifiedPages: make(map[int]*MemPage),
		src:           src,
		dst:           dst,
	}
}

func (r *ReservedPager) Pager() Pager {
	return r.pager
}

// Reserve returns a pager reservation in the specified mode
func (p *pager) Reserve(mode Mode) *ReservedPager {
	reserved := &ReservedPager{
		releaseOnce: &sync.Once{},
		upgradeOnce: &sync.Once{},
		pager:       p,
	}

	switch mode {
	case ModeRead:
		p.mu.RLock()
		p.state = StateRead
		upgraded := false

		reserved.Release = func() {
			reserved.releaseOnce.Do(func() {
				if upgraded {
					p.mu.Unlock()
				} else {
					p.mu.RUnlock()
				}
			})
		}

		reserved.Upgrade = func() {
			reserved.upgradeOnce.Do(func() {
				upgraded = true
				// TODO: Go does not support upgradable locks.
				p.mu.RUnlock()
				// TODO: Possible race condition here where the db could have been written to.
				p.mu.Lock()
				p.state = StateWrite
			})
		}

	case ModeWrite:
		p.mu.Lock()
		p.state = StateWrite

		reserved.Release = func() {
			p.state = StateRead
			p.mu.Unlock()
		}

		reserved.Upgrade = func() {}
	}

	return reserved
}

// PageSize returns the page size of the pager
func (p *pager) PageSize() int {
	return p.pageSize
}

// State returns the current state of the pager
func (p *pager) State() State {
	return p.state
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

// Write caches pages in a dirty state
func (p *pager) Write(pages ...*MemPage) error {
	if p.state != StateWrite {
		return errors.New("cannot modify pager in read state")
	}

	for _, pg := range pages {
		p.pageCache[pg.Number()] = pg
	}

	return nil
}

// Flush flushes all dirty pages to destination
func (p *pager) Flush() error {
	if p.state != StateWrite {
		return errors.New("cannot modify pager in read state")
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

	return nil
}

// Reset clears all dirty pages
func (p *pager) Reset() {
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
	if p.state != StateWrite {
		return nil, errors.New("cannot modify pager in read state")
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
