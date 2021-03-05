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
	headerOffset := 0
	if p.PageNumber == 1 {
		headerOffset = 100
	}

	if _, err := w.Write(p.Data[headerOffset:]); err != nil {
		return err
	}

	return nil
}

func (p *MemPage) calculateOffsets(count int) (uint16, uint16) {
	cellOffsetPointer := 8 + p.NumCells*2
	if p.PageNumber == 1 {
		cellOffsetPointer = cellOffsetPointer + 100
	}

	recordLength := uint16(count)
	cellOffset := p.CellsOffset - recordLength

	return cellOffsetPointer, cellOffset
}

func (p *MemPage) Fits(count int) bool {
	cellOffsetPointer, cellOffset := p.calculateOffsets(count)
	return cellOffsetPointer+2 <= cellOffset
}

func (p *MemPage) AddCell(data []byte) {
	headerOffset := 0
	if p.PageNumber == 1 {
		headerOffset = 100
	}

	headerLen := 8
	if p.Type == PageTypeInternal || p.Type == PageTypeInternalIndex {
		headerLen = 12
	}

	cellOffsetPointer := headerLen + int(p.NumCells*2)
	if p.PageNumber == 1 {
		cellOffsetPointer = cellOffsetPointer + headerOffset
	}

	cellLength := uint16(len(data))
	cellOffset := p.CellsOffset - cellLength

	// Write a pointer to the new cell
	binary.BigEndian.PutUint16(p.Data[cellOffsetPointer:], cellOffset)

	// Write the data to the cell pointer
	copy(p.Data[cellOffset:], data)

	// Update cells offset for the next page
	p.CellsOffset = cellOffset

	// Update number of cells in this page
	p.NumCells = p.NumCells + 1

	// Update the header
	header := p.Data[headerOffset:]
	header[0] = byte(p.Type)
	binary.BigEndian.PutUint16(header[1:3], p.FreeBlock)
	binary.BigEndian.PutUint16(header[3:5], p.NumCells)
	binary.BigEndian.PutUint16(header[5:7], p.CellsOffset)
	header[7] = p.FragmentedFreeBytes

	if p.Type == PageTypeInternal || p.Type == PageTypeInternalIndex {
		binary.BigEndian.PutUint32(header[8:12], uint32(p.RightPage))
	}
}

func (p *MemPage) CopyTo(dst *MemPage) {
	dst.PageHeader = p.PageHeader
	copy(dst.Data, p.Data)
}
