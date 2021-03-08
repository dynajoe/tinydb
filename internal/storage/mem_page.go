package storage

import (
	"bytes"
	"encoding/binary"
	"io"
)

// NewPageHeader creates a new PageHeader
func NewPageHeader(pageType PageType, pageSize int) PageHeader {
	return PageHeader{
		Type:                pageType,
		CellsOffset:         uint16(pageSize),
		FreeBlock:           0,
		NumCells:            0,
		FragmentedFreeBytes: 0,
		RightPage:           0,
	}
}

// InteriorHeaderLen is the length of an interior btree node
const InteriorHeaderLen = 12

// LeafHeaderLen is the length of a btree leaf node
const LeafHeaderLen = 8

// PageType type of page. See associated enumeration values.
type PageType byte

const (
	// PageTypeInternal internal table page
	PageTypeInternal PageType = 0x05

	// PageTypeLeaf leaf table page
	PageTypeLeaf PageType = 0x0D

	// PageTypeInternalIndex internal index page
	PageTypeInternalIndex PageType = 0x02

	// PageTypeLeafIndex leaf index page
	PageTypeLeafIndex PageType = 0x0A
)

// PageHeader contains metadata about the page
// BTree Page
// The 100-byte database file header (found on page 1 only)
// The 8 or 12 byte b-tree page header
// The cell pointer array
// Unallocated space
// The cell content area
// The reserved region.
//      The size of the reserved region is determined by the
//      one-byte unsigned integer found at an offset of 20 into
//      the database file header. The size of the reserved region is usually zero.
// Example First page header
// 0D (00 00) (00 01) (0F 8A) (00)
type PageHeader struct {
	// Type is the PageType for the page
	Type PageType

	// FreeBlock The two-byte integer at offset 1 gives the start of the first freeblock on the page, or is zero if there are no freeblocks.
	// A freeblock is a structure used to identify unallocated space within a b-tree page.
	// Freeblocks are organized as a chain. The first 2 bytes of a freeblock are a big-endian integer which is the offset in the b-tree page of the next freeblock in the chain, or zero if the freeblock is the last on the chain.
	FreeBlock uint16

	// NumCells is the number of cells stored in this page.
	NumCells uint16

	// CellsOffset the start of the cell content area. A zero value for this integer is interpreted as 65536.
	// If the page contains no cells, this field contains the value PageSize.
	CellsOffset uint16

	// FragmentedFreeBytes the number of fragmented free bytes within the cell content area.
	FragmentedFreeBytes byte

	// RightPage internal nodes only
	RightPage int
}

// MemPage represents a raw table page
type MemPage struct {
	header     PageHeader
	pageNumber int
	data       []byte
	dirty      bool
}

// Number is the page number
func (p *MemPage) Number() int {
	return p.pageNumber
}

// WriteTo writes the page to the specified writer
func (p *MemPage) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(p.data)
	return int64(n), err
}

// SetHeader sets the page header and marks the page as dirty.
func (p *MemPage) SetHeader(h PageHeader) {
	p.dirty = true
	p.header = h
	p.updateHeaderData()
}

// CopyTo copies the page data to dst and marks dst as dirty.
func (p *MemPage) CopyTo(dst *MemPage) {
	dst.dirty = true
	dst.header = p.header
	copy(dst.data, p.data)
}

// Fits determines if there's enough space in the page for a cell
// of the specified size.
func (p *MemPage) Fits(recordLen int) bool {
	// Where the cell pointer will be stored
	cellPointerOffset := cellPointersStart(p.header.Type, p.pageNumber) + int(p.header.NumCells)*2

	// Where cell data would start
	cellDataOffset := int(p.header.CellsOffset) - recordLen

	return cellPointerOffset+2 <= cellDataOffset
}

// CellCount the total number of cells in this page
func (p *MemPage) CellCount() int {
	return int(p.header.NumCells)
}

// ReadRecord returns a slice of bytes of the requested cell.
func (p *MemPage) ReadRecord(cellIndex int) (*Record, error) {
	cellDataStart := p.cellDataOffset(cellIndex)

	// TODO: Should this be capped upper and lower bound?
	reader := bytes.NewReader(p.data[cellDataStart:])
	return ReadRecord(reader)
}

// ReadInteriorNode returns a slice of bytes of the requested cell.
func (p *MemPage) ReadInteriorNode(cellIndex int) (*InteriorNode, error) {
	cellDataStart := p.cellDataOffset(cellIndex)

	// TODO: Should this be capped upper and lower bound?
	return ReadInteriorNode(p.data[cellDataStart:])
}

// AddCell adds a cell entry to the page. This function assumes
// that the page can fit the new cell.
func (p *MemPage) AddCell(data []byte) {
	// Every cell is 2 bytes
	cellPointerOffset := cellPointersStart(p.header.Type, p.pageNumber) + int(2*p.header.NumCells)

	cellLength := uint16(len(data))
	cellOffset := p.header.CellsOffset - cellLength

	// Write a pointer to the new cell
	binary.BigEndian.PutUint16(p.data[cellPointerOffset:], cellOffset)

	// Write the data to the cell pointer
	copy(p.data[cellOffset:], data)

	// Update cells offset for the next page
	p.header.CellsOffset = cellOffset

	// Update number of cells in this page
	p.header.NumCells = p.header.NumCells + 1

	// Update the header
	p.updateHeaderData()
}

func (p *MemPage) updateHeaderData() {
	headerOffset := headerOffset(p.pageNumber)
	header := p.data[headerOffset:]
	header[0] = byte(p.header.Type)
	binary.BigEndian.PutUint16(header[1:3], p.header.FreeBlock)
	binary.BigEndian.PutUint16(header[3:5], p.header.NumCells)
	binary.BigEndian.PutUint16(header[5:7], p.header.CellsOffset)
	header[7] = p.header.FragmentedFreeBytes

	if p.header.Type == PageTypeInternal || p.header.Type == PageTypeInternalIndex {
		binary.BigEndian.PutUint32(header[8:12], uint32(p.header.RightPage))
	}
}

func (p *MemPage) cellDataOffset(cellIndex int) int {
	// Every cell is 2 bytes
	cellPointerOffset := cellPointersStart(p.header.Type, p.pageNumber) + 2*cellIndex

	// Read 2 bytes at the pointer
	var cellDataOffset uint16
	reader := bytes.NewReader(p.data[cellPointerOffset : cellPointerOffset+2])
	binary.Read(reader, binary.BigEndian, &cellDataOffset)

	// This is where the cell data starts
	return int(cellDataOffset)
}

func cellPointersStart(pageType PageType, pageNumber int) int {
	if pageType == PageTypeInternal || pageType == PageTypeInternalIndex {
		return headerOffset(pageNumber) + InteriorHeaderLen
	}
	return headerOffset(pageNumber) + LeafHeaderLen
}

func headerOffset(pageNumber int) int {
	if pageNumber == 1 {
		return 100
	}
	return 0
}

// FromBytes parses a byte slice to a MemPage and takes ownership of the slice.
func FromBytes(pageNumber int, data []byte) (*MemPage, error) {
	offset := headerOffset(pageNumber)

	view := data[offset:]
	header := PageHeader{
		Type:                PageType(view[0]),
		FreeBlock:           binary.BigEndian.Uint16(view[1:3]),
		NumCells:            binary.BigEndian.Uint16(view[3:5]),
		CellsOffset:         binary.BigEndian.Uint16(view[5:7]),
		FragmentedFreeBytes: view[7],
		RightPage:           0,
	}
	if header.Type == PageTypeInternal || header.Type == PageTypeInternalIndex {
		header.RightPage = int(binary.BigEndian.Uint32(view[8:12]))
	}

	return &MemPage{
		header:     header,
		pageNumber: pageNumber,
		data:       data,
		dirty:      false,
	}, nil
}
