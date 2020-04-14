package storage

import (
	"encoding/binary"
	"io"
)

// MemPage represents a raw table page, the data is all
type MemPage struct {
	PageHeader
	PageNumber int
	Data       []byte
}

func (p *MemPage) Write(w io.Writer) error {
	// TODO: handle non-leaf page types
	if p.Type != PageTypeLeaf && p.Type != PageTypeLeafIndex {
		panic("unhandled page type")
	}

	// Page one is a special case
	// The page header starts after the file header
	// It is expected that the page header
	// is already written to the data at indexes 0-99
	offset := 0
	if p.PageNumber == 1 {
		offset = 100
	}

	header := p.Data[offset : offset+8]
	header[0] = byte(p.Type)
	binary.BigEndian.PutUint16(header[1:3], p.FreeBlock)
	binary.BigEndian.PutUint16(header[3:5], p.NumCells)
	binary.BigEndian.PutUint16(header[5:7], p.CellsOffset)
	header[7] = 0 // Always zero

	if _, err := w.Write(p.Data); err != nil {
		return err
	}
	return nil
}
