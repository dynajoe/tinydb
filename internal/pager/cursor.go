package pager

import (
	"errors"

	"github.com/joeandaverde/tinydb/internal/storage"
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
	rootPage int

	currentPage int
	cellIndex   int

	parentIndex int
	parentPage  int

	pager Pager
}

// NewCursor initializes a cursor to traverse the database btree
func NewCursor(pager Pager, typ CursorType, rootPage int, name string) (*Cursor, error) {
	return &Cursor{
		Name:        name,
		pager:       pager,
		rootPage:    rootPage,
		currentPage: rootPage,
		parentPage:  0,
		parentIndex: 0,
		cellIndex:   0,
		typ:         typ,
	}, nil
}

// CurrentCell reads the current record
func (c *Cursor) CurrentCell() (*storage.Record, error) {
	p, err := c.pager.Read(c.currentPage)
	if err != nil {
		return nil, err
	}

	// Attempt to access non-leaf cell
	if p.header.Type != PageTypeLeaf {
		return nil, errors.New("expected current position to be on leaf node")
	}

	return p.ReadRecord(c.cellIndex)
}

// Insert places a record in the btree
func (c *Cursor) Insert(record *storage.Record) error {
	btreeTable := NewBTreeTable(c.rootPage, c.pager)
	return btreeTable.Insert(record)
}

// Next advances the cursor to the next record
// returns true if there is a record false otherwise
func (c *Cursor) Next() (bool, error) {
	p, err := c.pager.Read(c.currentPage)
	if err != nil {
		return false, err
	}

	nextIndex := c.cellIndex + 1

	// Encountering an internal page should traverse its children
	if p.header.Type == PageTypeInternal {
		if nextIndex < int(p.header.NumCells) {
			interiorNode, err := p.ReadInteriorNode(nextIndex)
			if err != nil {
				return false, err
			}

			nextPage := int(interiorNode.LeftChild)

			// Store the position in the parent
			// This may need to become a stack or linked list.
			c.parentPage = p.Number()
			c.parentIndex = nextIndex

			// Start at the beginning of the child node
			c.currentPage = nextPage
			c.cellIndex = -1
		} else {
			// TODO: Confirm intended behavior of right page

			// The last node to be traversed.
			c.parentPage = 0
			c.parentIndex = 0

			// Last page is the right page.
			c.currentPage = p.header.RightPage
			c.cellIndex = -1
		}

		return c.Next()
	}

	// If leaf has been completely traversed.
	// Go go to next leaf or done.
	if nextIndex >= int(p.header.NumCells) {
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
	c.cellIndex = -1
	c.parentIndex = 0
	c.parentPage = 0
	return c.Next()
}
