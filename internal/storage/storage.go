package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
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
}

func Open(path string) (*Pager, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	// The file was created.
	// Initialize the database file.
	if info.Size() == 0 {
		return initDataFile(file)
	}

	return NewPager(file), nil
}

func initDataFile(file *os.File) (*Pager, error) {
	header := NewFileHeader()
	if err := header.Write(file); err != nil {
		return nil, err
	}

	// Add a page to file to store the schema table
	pager := NewPager(file)
	if err := pager.Allocate(); err != nil {
		return nil, err
	}

	// Initialize page one
	pageOne := NewPage(1, header.PageSize)
	if err := pager.Write(pageOne); err != nil {
		return nil, err
	}

	// Page 1 of a database file is the root page of a table b-tree that
	// holds a special table named "sqlite_master" (or "sqlite_temp_master" in
	// the case of a TEMP database) which stores the complete database schema.
	// The structure of the sqlite_master table is as if it had been
	// created using the following SQL:
	//
	// CREATE TABLE sqlite_master(
	//    type text,
	//    name text,
	//    tbl_name text,
	//    rootpage integer,
	//    sql text
	// );

	// fsync
	if err := file.Sync(); err != nil {
		return nil, err
	}

	return pager, nil
}

// NewFileHeader creates a new FileHeader
func NewFileHeader() FileHeader {
	return FileHeader{
		PageSize:          4096,
		FileChangeCounter: 0,
		SchemaVersion:     0,
		PageCacheSize:     20000,
		UserCookie:        0,
	}
}

// Write writes FileHeader to a file
func (h FileHeader) Write(w io.Writer) error {
	header := make([]byte, 100, 100)

	copy(header, []byte("SQLite format 3\000"))

	// PageSize: The two-byte value beginning at offset 16 determines the page size of the database.
	// For SQLite versions 3.7.0.1 (2010-08-04) and earlier, this value is interpreted as a
	// big-endian integer and must be a power of two between 512 and 32768, inclusive.
	// Beginning with SQLite version 3.7.1 (2010-08-23), a page size of 65536 bytes is supported.
	// The value 65536 will not fit in a two-byte integer, so to specify a 65536-byte page size,
	// the value at offset 16 is 0x00 0x01. This value can be interpreted as a big-endian 1 and thought
	// of as a magic number to represent the 65536 page size. Or one can view the two-byte field as a
	// little endian number and say that it represents the page size divided by 256. These two interpretations of
	// the page-size field are equivalent.
	binary.BigEndian.PutUint16(header[16:], h.PageSize)

	// 18	1	File format write version. 1 for legacy; 2 for WAL.
	header[18] = 2
	// 19	1	File format read version. 1 for legacy; 2 for WAL.
	header[19] = 2
	// 20	1	Bytes of unused "reserved" space at the end of each page. Usually 0.
	header[20] = 0
	// 21	1	Maximum embedded payload fraction. Must be 64.
	header[21] = 64
	// 22	1	Minimum embedded payload fraction. Must be 32.
	header[22] = 32
	// 23	1	Leaf payload fraction. Must be 32.
	header[23] = 32

	binary.BigEndian.PutUint32(header[24:], h.FileChangeCounter)
	binary.BigEndian.PutUint32(header[32:], 0)
	binary.BigEndian.PutUint32(header[36:], 0)
	binary.BigEndian.PutUint32(header[40:], h.SchemaVersion)
	binary.BigEndian.PutUint32(header[44:], 1)
	binary.BigEndian.PutUint32(header[48:], h.PageCacheSize)
	binary.BigEndian.PutUint32(header[52:], 0)
	binary.BigEndian.PutUint32(header[56:], 1)
	binary.BigEndian.PutUint32(header[60:], h.UserCookie)
	binary.BigEndian.PutUint32(header[64:], 0)

	if _, err := w.Write(header); err != nil {
		return err
	}

	return nil
}

// ToBytes encodes PageHeader
func (h PageHeader) Write(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, h)
}

// ReadHeader deserializes a FileHeader
func ReadHeader(r io.Reader) FileHeader {
	buf := make([]byte, 100)
	if n, err := r.Read(buf); err != nil || n < 100 {
		panic("unexpected header buffer size")
	}

	return FileHeader{
		PageSize:          binary.BigEndian.Uint16(buf[16:18]),
		FileChangeCounter: binary.BigEndian.Uint32(buf[24:28]),
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
		NumCells:    0,
		FreeOffset:  0,
	}
}

// PageType type of page. See associated enumeration values.
type PageType uint8

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

// PageHeader contains metadata about the page
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

	// Always zero for serialization
	xxZero uint8

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
	if err := binary.Write(w, binary.BigEndian, p.PageHeader); err != nil {
		return err
	}
	if _, err := w.Write(p.Data); err != nil {
		return err
	}
	return nil
}

func ReadPage(reader io.Reader, pageSize uint16) (*MemPage, error) {
	page := make([]byte, pageSize)

	bytesRead, err := reader.Read(page)
	if err != nil {
		return nil, err
	}
	if bytesRead != int(pageSize) {
		return nil, errors.New("unexpected page size")
	}

	tablePage := MemPage{
		PageHeader: PageHeader{
			Type:        PageType(page[0]),
			FreeOffset:  binary.BigEndian.Uint16(page[1:3]),
			NumCells:    binary.BigEndian.Uint16(page[3:5]),
			CellsOffset: binary.BigEndian.Uint16(page[5:7]),
			RightPage:   binary.BigEndian.Uint32(page[8:11]),
		},
		Data: page[12:],
	}

	return &tablePage, nil
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

// ToBytes serializes a database record for storage
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

	// Write the cell pointer
	binary.BigEndian.PutUint16(p.Data[p.FreeOffset:], newCellsOffset)

	// Take two bytes from freespace for the cell pointer
	p.FreeOffset = p.FreeOffset + 2 // 2 bytes

	// Move cells offset into freespace
	p.CellsOffset = newCellsOffset

	// Increase number of stored cells in this page
	p.NumCells = p.NumCells + 1

	return nil
}
