package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type CursorType byte

const (
	CURSOR_UNKNOWN = 0
	CURSOR_READ    = 1
	CURSOR_WRITE   = 2
)

type Cursor struct {
	Name string

	typ      CursorType
	start    int64
	rootPage int

	currentPage int
	cellIndex   int

	parentIndex int
	parentPage  int

	pager Pager
	wal   *WAL
}

// NewCursor initializes a cursor to traverse the database btree
func NewCursor(pager Pager, wal *WAL, typ CursorType, rootPage int, name string) (*Cursor, error) {
	return &Cursor{
		Name:        name,
		pager:       pager,
		wal:         wal,
		rootPage:    rootPage,
		currentPage: rootPage,
		parentPage:  0,
		parentIndex: 0,
		cellIndex:   0,
		typ:         typ,
	}, nil
}

// CurrentCell reads the current record
func (c *Cursor) CurrentCell() (*Record, error) {
	p, err := c.readCurrentPage()
	if err != nil {
		return nil, err
	}

	// Attempt to access non-leaf cell
	if p.Type != PageTypeLeaf {
		return nil, errors.New("expected current position to be on leaf node")
	}

	// Read data at the current cell
	ptr, err := readCellPointer(c.cellIndex, p)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(p.Data[ptr:])
	return ReadRecord(reader)
}

// Insert places a record in the btree
func (c *Cursor) Insert(record *Record) error {
	btreeTable := NewBTreeTable(c.rootPage, c.pager, c.wal)
	return btreeTable.Insert(record)
}

// Next advances the cursor to the next record
// returns true if there is a record false otherwise
func (c *Cursor) Next() (bool, error) {
	p, err := c.readCurrentPage()
	if err != nil {
		return false, err
	}

	// Encountering an internal page should traverse its children
	if p.Type == PageTypeInternal {
		if c.cellIndex < int(p.NumCells) {
			ptr, err := readCellPointer(c.cellIndex, p)
			if err != nil {
				return false, nil
			}
			interiorNode, _ := ReadInteriorNode(p.Data[ptr:])
			nextPage := int(interiorNode.LeftChild)

			// Store the position in the parent
			// This may need to become a stack or linked list.
			c.parentPage = p.PageNumber
			parentIndex := c.cellIndex + 1
			c.parentIndex = parentIndex

			// Start at the beginning of the child node
			c.currentPage = nextPage
			c.cellIndex = 0
		} else {
			// TODO: Confirm intended behavior of right page

			// The last node to be traversed.
			c.parentPage = 0
			c.parentIndex = 0

			// Last page is the right page.
			c.currentPage = p.RightPage
			c.cellIndex = 0
		}

		return c.Next()
	}

	nextIndex := c.cellIndex + 1

	// If leaf has been completely traversed.
	// Go go to next leaf or done.
	if nextIndex >= int(p.NumCells) {
		// No parent, we're done.
		if c.parentPage == 0 {
			return false, nil
		}

		// Restore parent interior node position
		c.currentPage = c.parentPage
		c.cellIndex = c.parentIndex

		// Start at next child in parent
		return c.Next()
	}

	c.cellIndex = nextIndex
	return true, nil
}

// Rewind sets the cursor to the first entry in the btree
// returns true if there is a record false otherwise
func (c *Cursor) Rewind() (bool, error) {
	c.currentPage = c.rootPage
	c.cellIndex = 0
	c.parentIndex = 0
	c.parentPage = 0
	return c.Next()
}

// readPage reads the current page from the wal or the database file.
func (c *Cursor) readCurrentPage() (*MemPage, error) {
	return c.pager.Read(c.currentPage)
}

func readCellPointer(cellIndex int, page *MemPage) (uint16, error) {
	headerLen := 8
	if page.Type == PageTypeInternal {
		headerLen = 12
	}

	offset := headerLen
	if page.PageNumber == 1 {
		offset += 100
	}

	cellOffset := offset + cellIndex*2
	return binary.BigEndian.Uint16(page.Data[cellOffset:]), nil
}
