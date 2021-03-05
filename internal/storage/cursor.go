package storage

import (
	"bytes"
	"encoding/binary"
)

type CursorType byte

const (
	CURSOR_UNKNOWN = 0
	CURSOR_READ    = 1
	CURSOR_WRITE   = 2
)

type Cursor struct {
	Name string

	typ        CursorType
	start      int64
	pageNumber int
	cellIndex  int
	pager      Pager
	wal        *WAL
	memPage    *MemPage
}

func NewCursor(pager Pager, wal *WAL, typ CursorType, pageNumber int, name string) (*Cursor, error) {
	pg, err := pager.Read(pageNumber)
	if err != nil {
		return nil, err
	}
	return &Cursor{
		Name:       name,
		pager:      pager,
		wal:        wal,
		pageNumber: pageNumber,
		memPage:    pg,
		cellIndex:  0,
		typ:        typ,
	}, nil
}

func (p *Cursor) Close() {

}

func (c *Cursor) CurrentCell() (*Record, error) {
	// TODO: assumes always leaf page
	offset := 8
	if c.memPage.PageNumber == 1 {
		offset = offset + 100
	}

	cellOffset := offset + c.cellIndex*2
	cellPtr := binary.BigEndian.Uint16(c.memPage.Data[cellOffset : cellOffset+2])
	reader := bytes.NewReader(c.memPage.Data[cellPtr:])

	return ReadRecord(reader)
}

func (c *Cursor) Insert(record *Record) error {
	btreeTable := NewBTreeTable(c.pageNumber, c.pager, c.wal)
	return btreeTable.Insert(record)
}

// Next advances the cursor to the next record
// returns true if there is a record false otherwise
// TODO: this doesn't support navigating the btree and assumes a table
// fits on a single page.
func (c *Cursor) Next() (bool, error) {
	nextIndex := c.cellIndex + 1
	if nextIndex >= int(c.memPage.NumCells) {
		return false, nil
	}
	c.cellIndex = nextIndex

	return true, nil
}

// Rewind sets the cursor to the first entry in the btree
// returns true if there is a record false otherwise
func (c *Cursor) Rewind() (bool, error) {
	if c.memPage.NumCells == 0 {
		return false, nil
	}

	c.cellIndex = 0
	return true, nil
}
