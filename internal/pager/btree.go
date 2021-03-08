package pager

import (
	"bytes"
	"errors"

	"github.com/joeandaverde/tinydb/internal/storage"
)

type BTreeTable struct {
	rootPage int
	pager    Pager
}

func NewBTreeTable(rootPage int, p Pager) *BTreeTable {
	return &BTreeTable{
		rootPage: rootPage,
		pager:    p,
	}
}

func (b *BTreeTable) Insert(r *storage.Record) error {
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

	if root.header.Type == PageTypeLeaf {
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
	} else if root.header.Type == PageTypeInternal {
		// TODO: For now, just write to the right most page.
		destPage, err := b.pager.Read(root.header.RightPage)
		if err != nil {
			return err
		}

		// If the rightmost page is full, create a new page and update the pointer.
		if !destPage.Fits(len(recordBytes)) {
			maxRowID, err := maxRowID(destPage)
			if err != nil {
				return err
			}

			internalNode := storage.InteriorNode{
				LeftChild: uint32(destPage.Number()),
				Key:       maxRowID,
			}

			if !root.Fits(storage.InteriorNodeSize) {
				return errors.New("not yet supporting adding another internal node")
			}

			// Allocate a new page, update internal node right pointer.
			destPage, err = b.pager.Allocate(PageTypeLeaf) //Leaf
			if err != nil {
				return err
			}
			root.header.RightPage = destPage.Number()

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

	// Copy the page to the left
	p.CopyTo(leftPage)

	// Update the header to make the page an interior node
	newHeader := NewPageHeader(PageTypeInternal, pager.PageSize())
	newHeader.RightPage = rightPage.Number()
	p.SetHeader(newHeader)

	// Add a cell to the new internal page
	cell := storage.InteriorNode{
		LeftChild: uint32(leftPage.Number()),
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
	recordIter := newRecordIter(p)
	maxRowID := uint32(0)

	for recordIter.Next() {
		if recordIter.Error() != nil {
			return 0, recordIter.Error()
		}

		record := recordIter.Current()
		if record.RowID > maxRowID {
			maxRowID = record.RowID
		}
	}
	return maxRowID, nil
}

type recorditerator struct {
	n      int
	record *storage.Record
	p      *MemPage
	err    error
}

func newRecordIter(p *MemPage) *recorditerator {
	return &recorditerator{p: p, n: 0}
}

func (i *recorditerator) Next() bool {
	if i.n < i.p.CellCount() {
		i.record, i.err = i.p.ReadRecord(i.n)
		i.n++
		return true
	}

	return false
}

func (i *recorditerator) Error() error {
	return i.err
}

func (i *recorditerator) Current() *storage.Record {
	return i.record
}
