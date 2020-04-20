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
	typ        CursorType
	start      int64
	pageNumber int
	cellIndex  int
	pager      *Pager
}

func (c *Cursor) CurrentCell() (Record, error) {
	page, err := c.pager.Read(c.pageNumber)
	if err != nil {
		return Record{}, err
	}

	// TODO: assumes always leaf page
	offset := 8
	if page.PageNumber == 1 {
		offset = offset + 100
	}

	cellOffset := offset + c.cellIndex*2
	cellPtr := binary.BigEndian.Uint16(page.Data[cellOffset : cellOffset+2])
	reader := bytes.NewReader(page.Data[cellPtr:])

	return ReadRecord(reader)
}

func (c *Cursor) Insert(rowID int, record Record) error {
	rootPage, err := c.pager.Read(c.pageNumber)
	if err != nil {
		return err
	}
	if err := WriteRecord(rootPage, rowID, record); err != nil {
		return err
	}
	if err := c.pager.Write(rootPage); err != nil {
		return err
	}

	return nil
}

func (c *Cursor) Next() {
	c.cellIndex = c.cellIndex + 1
}

func (c *Cursor) Rewind() error {
	return nil
}
