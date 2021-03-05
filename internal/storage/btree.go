package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

type BTreeTable struct {
	rootPage int
	pager    Pager
	wal      *WAL
}

const InteriorNodeSize int = 8

type InteriorNode struct {
	LeftChild uint32
	Key       uint32
}

func (r InteriorNode) ToBytes() ([]byte, error) {
	buf := bytes.Buffer{}
	if err := r.Write(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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

func ReadInteriorNode(data []byte) (InteriorNode, error) {
	reader := bytes.NewReader(data)

	var leftChild uint32
	if err := binary.Read(reader, binary.BigEndian, &leftChild); err != nil {
		return InteriorNode{}, err
	}

	key, _, err := ReadVarint(reader)
	if err != nil {
		return InteriorNode{}, err
	}

	return InteriorNode{LeftChild: leftChild, Key: uint32(key)}, nil
}

func NewBTreeTable(rootPage int, p Pager, wal *WAL) *BTreeTable {
	return &BTreeTable{
		wal:      wal,
		rootPage: rootPage,
		pager:    p,
	}
}

func (b *BTreeTable) Insert(r *Record) error {
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

	if root.Type == PageTypeLeaf {
		if !root.Fits(len(recordBytes)) {
			parent, left, right, err := splitPage(b.pager, root)
			if err != nil {
				return err
			}

			// Write the record to the right page
			right.AddCell(recordBytes)

			// Write all pages to disk
			return b.pager.Write(left, right, parent)
		}

		// Write the record to the leaf page
		root.AddCell(recordBytes)

		// Save the page
		return b.pager.Write(root)
	} else if root.Type == PageTypeInternal {
		// TODO: For now, just write to the right most page.
		destPage, err := b.pager.Read(root.RightPage)
		if err != nil {
			return err
		}

		// If the rightmost page is full, create a new page and update the pointer.
		if !destPage.Fits(len(recordBytes)) {
			maxRowID, err := maxRowID(destPage)
			if err != nil {
				return err
			}

			internalNode := InteriorNode{
				LeftChild: uint32(destPage.PageNumber),
				Key:       maxRowID,
			}

			if !root.Fits(InteriorNodeSize) {
				return errors.New("not yet supporting adding another internal node")
			}

			// Allocate a new page, update internal node right pointer.
			destPage, err = b.pager.Allocate(PageTypeLeaf)
			if err != nil {
				return err
			}
			root.RightPage = destPage.PageNumber

			// Add link to the newly added page.
			interiorCell, err := internalNode.ToBytes()
			if err != nil {
				return err
			}
			root.AddCell(interiorCell)

			// Write the record
			destPage.AddCell(recordBytes)
			return b.pager.Write(root, destPage)
		}

		// Write the record
		destPage.AddCell(recordBytes)
		return b.pager.Write(destPage)
	} else {
		return errors.New("unsupported page type")
	}
}

func splitPage(pager Pager, p *MemPage) (*MemPage, *MemPage, *MemPage, error) {
	// New page for the left node
	leftPage, err := pager.Allocate(PageTypeLeaf)
	if err != nil {
		return nil, nil, nil, err
	}

	// New page for the right node
	rightPage, err := pager.Allocate(PageTypeLeaf)
	if err != nil {
		return nil, nil, nil, err
	}

	// Max key
	maxRowID, err := maxRowID(p)
	if err != nil {
		return nil, nil, nil, err
	}

	// Copy the data to the left
	p.CopyTo(leftPage)

	// Update the header to make the root an internal page
	p.PageHeader = NewPageHeader(PageTypeInternal, uint16(len(p.Data)))
	p.PageHeader.RightPage = rightPage.PageNumber

	// Add a cell to the new internal page
	cell := InteriorNode{
		LeftChild: uint32(leftPage.PageNumber),
		Key:       maxRowID,
	}

	cellBytes, err := cell.ToBytes()
	if err != nil {
		return nil, nil, nil, err
	}
	p.AddCell(cellBytes)

	return p, leftPage, rightPage, nil
}

func maxRowID(p *MemPage) (uint32, error) {
	rows := RowReader(p)
	maxRowID := uint32(0)
	for row := range rows {
		if row.Err != nil {
			return 0, row.Err
		}

		if row.Record.RowID > maxRowID {
			maxRowID = row.Record.RowID
		}
	}

	return maxRowID, nil
}
