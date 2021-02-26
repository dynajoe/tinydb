package storage

import (
	"bytes"
	"encoding/binary"
	"io"
)

type BTreeTable struct {
	rootPage int
	pager    Pager
	wal      *WAL
}

type InteriorNode struct {
	LeftChild uint32
	Key       uint32
}

// Write writes an interior node to the specified writer
func (r InteriorNode) Write(bs io.ByteWriter) error {
	recordBuffer := bytes.Buffer{}

	// Write the child page
	if err := binary.Write(&recordBuffer, binary.BigEndian, r.LeftChild); err != nil {
		return err
	}

	// Write the key
	WriteVarint(&recordBuffer, uint64(r.Key))

	// Write to the byte writer
	// TODO: this seems ineffective.
	for _, b := range recordBuffer.Bytes() {
		if err := bs.WriteByte(b); err != nil {
			return err
		}
	}

	return nil
}

func NewBTreeTable(rootPage int, p Pager, wal *WAL) *BTreeTable {
	return &BTreeTable{
		wal:      wal,
		rootPage: rootPage,
		pager:    p,
	}
}

func (b *BTreeTable) Insert(r Record) error {
	buf := bytes.Buffer{}
	if err := r.Write(&buf); err != nil {
		return err
	}
	recordBytes := buf.Bytes()

	// Load the table root page
	root, err := b.pager.Read(b.rootPage)
	if err != nil {
		return err
	}

	// Append to WAL
	b.wal.Append()

	// Only support leaf pages at root for now.
	if root.Type != PageTypeLeaf {
		panic("traversing internal node not supported yet")
	}

	if !root.Fits(recordBytes) {
		if root.PageNumber == 1 {
			panic("splitting page 1 not supported")
		}

		// Split the page
		// TODO: Properly execute btree algorithm
		parent, left, right, err := splitPage(b.pager, root)
		if err != nil {
			return err
		}

		// Write the record to the right page
		right.AddCell(recordBytes)

		// Write all pages to disk
		return b.pager.Write(left, right, parent)
	}

	root.AddCell(recordBytes)
	return b.pager.Write(root)
}

func splitPage(pager Pager, p *MemPage) (*MemPage, *MemPage, *MemPage, error) {
	parentPageNumber := p.PageNumber

	// Split the root node into two leaf nodes
	leftPageNumber := pager.Reserve()
	p.PageNumber = leftPageNumber
	leftPage := p

	rightPage, err := pager.Allocate()
	if err != nil {
		return nil, nil, nil, err
	}

	// Create a new root with pointers
	rows := RowReader(p)
	maxRowID := uint32(0)
	for row := range rows {
		if row.RowID > maxRowID {
			maxRowID = row.RowID
		}
	}

	newRoot := makeInteriorPage(parentPageNumber, leftPage, rightPage)
	internalNode := InteriorNode{
		LeftChild: uint32(leftPage.PageNumber),
		Key:       maxRowID,
	}

	buf := bytes.Buffer{}
	internalNode.Write(&buf)
	newRoot.AddCell(buf.Bytes())
	return newRoot, leftPage, rightPage, nil
}
