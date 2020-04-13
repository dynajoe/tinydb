package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// FileHeader represents a database file header
type FileHeader struct {
	// 16-17	PageSize	uint16	Size of database page
	PageSize uint16
	// 24-27	FileChangeCounter	uint32	Initialized to 0. Each time a modification is made to the database, this counter is increased.
	FileChangeCounter uint32
	// 40-43	SchemaVersion	uint32	Initialized to 0. Each time the database schema is modified, this counter is increased.
	SchemaVersion uint32
	// 48-51	PageCacheSize	uint32	Default pager cache size in bytes. Initialized to 20000
	PageCacheSize uint32
	// 60-43	UserCookie	uint32	Available to the user for read-write access. Initialized to 0
	UserCookie uint32
	// Size in pages of the database
	SizeInPages uint32
}

// NewFileHeader creates a new FileHeader
func NewFileHeader() FileHeader {
	return FileHeader{
		PageSize:          4096,
		FileChangeCounter: 0,
		SchemaVersion:     0,
		PageCacheSize:     20000,
		UserCookie:        0,
		SizeInPages:       1,
	}
}

// WriteTo writes FileHeader to the provided io.WriterTo
func (h FileHeader) WriteTo(w io.Writer) (int64, error) {
	data := make([]byte, 100)
	copy(data, "SQLite format 3\000")

	// PageSize: The two-byte value beginning at offset 16 determines the page size of the database.
	// For SQLite versions 3.7.0.1 (2010-08-04) and earlier, this value is interpreted as a
	// big-endian integer and must be a power of two between 512 and 32768, inclusive.
	// Beginning with SQLite version 3.7.1 (2010-08-23), a page size of 65536 bytes is supported.
	// The value 65536 will not fit in a two-byte integer, so to specify a 65536-byte page size,
	// the value at offset 16 is 0x00 0x01. This value can be interpreted as a big-endian 1 and thought
	// of as a magic number to represent the 65536 page size. Or one can view the two-byte field as a
	// little endian number and say that it represents the page size divided by 256. These two interpretations of
	// the page-size field are equivalent.
	binary.BigEndian.PutUint16(data[16:], h.PageSize)

	// 18	1	File format write version. 1 for legacy; 2 for WAL.
	data[18] = 1
	// 19	1	File format read version. 1 for legacy; 2 for WAL.
	data[19] = 1
	// 20	1	Bytes of unused "reserved" space at the end of each page. Usually 0.
	data[20] = 0
	// 21	1	Maximum embedded payload fraction. Must be 64.
	data[21] = 64
	// 22	1	Minimum embedded payload fraction. Must be 32.
	data[22] = 32
	// 23	1	Leaf payload fraction. Must be 32.
	data[23] = 32

	binary.BigEndian.PutUint32(data[24:], h.FileChangeCounter)
	binary.BigEndian.PutUint32(data[28:], h.SizeInPages)
	binary.BigEndian.PutUint32(data[32:], 0)
	binary.BigEndian.PutUint32(data[36:], 0)
	binary.BigEndian.PutUint32(data[40:], h.SchemaVersion)
	binary.BigEndian.PutUint32(data[44:], 1)
	binary.BigEndian.PutUint32(data[48:], h.PageCacheSize)
	binary.BigEndian.PutUint32(data[52:], 0)
	binary.BigEndian.PutUint32(data[56:], 1)
	binary.BigEndian.PutUint32(data[60:], h.UserCookie)
	binary.BigEndian.PutUint32(data[64:], 0)

	if _, err := w.Write(data); err != nil {
		return 0, err
	}

	return 100, nil
}

// ParseFileHeader deserializes a FileHeader
func ParseFileHeader(buf []byte) FileHeader {
	if len(buf) != 100 {
		panic("unexpected header length")
	}

	return FileHeader{
		PageSize:          binary.BigEndian.Uint16(buf[16:18]),
		FileChangeCounter: binary.BigEndian.Uint32(buf[24:28]),
		SizeInPages:       binary.BigEndian.Uint32(buf[28:32]),
		SchemaVersion:     binary.BigEndian.Uint32(buf[40:44]),
		PageCacheSize:     binary.BigEndian.Uint32(buf[48:52]),
		UserCookie:        binary.BigEndian.Uint32(buf[60:64]),
	}
}

// NewPageHeader creates a new PageHeader
func NewPageHeader(t PageType, pageSize uint16) PageHeader {
	return PageHeader{
		Type:        t,
		CellsOffset: pageSize,
		FreeOffset:  0,
		NumCells:    0,
		RightPage:   0,
	}
}

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

	// FreeOffset is the byte offset at which the free space starts. Note that this must be updated every time the cell offset array grows.
	FreeOffset uint16

	// NumCells is the number of cells stored in this page.
	NumCells uint16

	// CellsOffset is the byte offset at which the cells start.
	// If the page contains no cells, this field contains the value PageSize.
	// This value must be updated every time a cell is added.
	CellsOffset uint16

	// RightPage internal nodes only
	RightPage uint32
}

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
	binary.BigEndian.PutUint16(header[1:3], p.FreeOffset)
	binary.BigEndian.PutUint16(header[3:5], p.NumCells)
	binary.BigEndian.PutUint16(header[5:7], p.CellsOffset)
	header[7] = 0 // Always zero

	if _, err := w.Write(p.Data); err != nil {
		return err
	}
	return nil
}

func ReadPage(page int, pageSize uint16, reader io.Reader) (*MemPage, error) {
	data := make([]byte, pageSize)

	bytesRead, err := reader.Read(data)
	if err != nil {
		return nil, err
	}
	if bytesRead != int(pageSize) {
		return nil, errors.New("unexpected page size")
	}

	// The header starts at offset 100 of page 1
	headerBytes := data
	if page == 1 {
		headerBytes = data[100:]
	}

	header := PageHeader{
		Type:        PageType(headerBytes[0]),
		FreeOffset:  binary.BigEndian.Uint16(headerBytes[1:3]),
		NumCells:    binary.BigEndian.Uint16(headerBytes[3:5]),
		CellsOffset: binary.BigEndian.Uint16(headerBytes[5:7]),
	}
	if header.Type == PageTypeInternal || header.Type == PageTypeInternalIndex {
		header.RightPage = binary.BigEndian.Uint32(headerBytes[8:11])
	}

	return &MemPage{
		PageHeader: header,
		PageNumber: page,
		Data:       data,
	}, nil
}

type InternalTableCell struct {
	ChildPage uint32
	Key       uint32
}

type LeafTableCell struct {
	Length uint32
	Key    uint32
	Data   []byte
}

type SQLType uint32

const (
	Byte     = 1
	SmallInt = 2
	Integer  = 4
	Key      = 24
	Text     = 28
)

// Record is a set of fields
type Record struct {
	Fields []Field
}

// Field is a field in a database record
type Field struct {
	Type SQLType
	Data interface{}
}

// NewRecord creates a database record from a set of fields
func NewRecord(fields []Field) Record {
	return Record{
		Fields: fields,
	}
}

// WriteTo writes a record to the specified writer
func (r *Record) Write(bs io.Writer) error {
	// Build the header [varint header size..., cols...]
	var colBuf bytes.Buffer
	for _, f := range r.Fields {
		// If data is nil indicate
		// the SQL type is NULL
		if f.Data == nil {
			colBuf.WriteByte(0)
			continue
		}

		switch f.Type {
		case Key:
			colBuf.WriteByte(0)
		case Byte:
			colBuf.WriteByte(1)
		case SmallInt:
			colBuf.WriteByte(2)
		case Integer:
			colBuf.WriteByte(4)
		case Text:
			fieldSize := uint32(2*len(f.Data.(string)) + 13)
			encodedSize := make([]byte, 4)
			bytesWritten := binary.PutVarint(encodedSize, int64(fieldSize))
			colBuf.Write(encodedSize[:bytesWritten])
		default:
			panic("Unknown sql type")
		}
	}

	// Size
	// TODO: this assumes the header size can fit into 7 bit integer, which is usually okay.
	// Add 1 because to include the first byte that includes size
	bs.Write([]byte{byte(colBuf.Len() + 1)})

	// Columns
	bs.Write(colBuf.Bytes())

	for _, f := range r.Fields {
		// Nil is specified handled in header
		// Key is in Header
		if f.Data == nil || f.Type == Key {
			continue
		}

		switch f.Data.(type) {
		case int8:
			bs.Write([]byte{byte(f.Data.(int8))})
		case byte:
			bs.Write([]byte{f.Data.(byte)})
		case int16:
			binary.Write(bs, binary.BigEndian, uint16(f.Data.(int16)))
		case int32:
			binary.Write(bs, binary.BigEndian, uint32(f.Data.(int32)))
		case int64:
			binary.Write(bs, binary.BigEndian, uint32(f.Data.(int64)))
		case int:
			binary.Write(bs, binary.BigEndian, uint32(f.Data.(int)))
		case string:
			bs.Write([]byte(f.Data.(string)))
		}
	}

	return nil
}

func WriteRecord(p *MemPage, r Record) error {
	buf := bytes.Buffer{}
	if err := r.Write(&buf); err != nil {
		return err
	}

	// TODO: this is wasteful
	recordBytes := buf.Bytes()
	newCellsOffset := p.CellsOffset - uint16(len(recordBytes))

	// Copy the record data starting at the new cells offset
	copy(p.Data[newCellsOffset:], recordBytes)

	// WriteTo the cell pointer
	binary.BigEndian.PutUint16(p.Data[p.FreeOffset:], newCellsOffset)

	// Take two bytes from freespace for the cell pointer
	p.FreeOffset = p.FreeOffset + 2 // 2 bytes

	// Move cells offset into freespace
	p.CellsOffset = newCellsOffset

	// Increase number of stored cells in this page
	p.NumCells = p.NumCells + 1

	return nil
}

func PageOffset(page int, pageSize uint16) int64 {
	return int64(page-1) * int64(pageSize)
}
