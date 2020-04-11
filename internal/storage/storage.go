package storage

import (
	"bytes"
	"encoding/binary"
	"io"
)

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

const fileFormat = "SQLite format 3\000"

func NewFileHeader() FileHeader {
	return FileHeader{
		PageSize:          100,
		FileChangeCounter: 0,
		SchemaVersion:     0,
		PageCacheSize:     20000,
		UserCookie:        0,
	}
}

func (h FileHeader) ToBytes() []byte {
	header := make([]byte, 100, 100)

	copy(header, []byte(fileFormat))

	// PageSize: The two-byte value beginning at offset 16 determines the page size of the database.
	// For SQLite versions 3.7.0.1 (2010-08-04) and earlier, this value is interpreted as a
	// big-endian integer and must be a power of two between 512 and 32768, inclusive.
	// Beginning with SQLite version 3.7.1 (2010-08-23), a page size of 65536 bytes is supported.
	// The value 65536 will not fit in a two-byte integer, so to specify a 65536-byte page size,
	// the value at offset 16 is 0x00 0x01. This value can be interpreted as a big-endian 1 and thought
	// of as a magic number to represent the 65536 page size. Or one can view the two-byte field as a
	// little endian number and say that it represents the page size divided by 256. These two interpretations of
	// the page-size field are equivalent.
	binary.LittleEndian.PutUint16(header[16:], h.PageSize)

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

	binary.LittleEndian.PutUint32(header[24:], h.FileChangeCounter)
	binary.LittleEndian.PutUint32(header[32:], 0)
	binary.LittleEndian.PutUint32(header[36:], 0)
	binary.LittleEndian.PutUint32(header[40:], h.SchemaVersion)
	binary.LittleEndian.PutUint32(header[44:], 1)
	binary.LittleEndian.PutUint32(header[48:], h.PageCacheSize)
	binary.LittleEndian.PutUint32(header[52:], 0)
	binary.LittleEndian.PutUint32(header[56:], 1)
	binary.LittleEndian.PutUint32(header[60:], h.UserCookie)
	binary.LittleEndian.PutUint32(header[64:], 0)

	return header
}

func (h PageHeader) ToBytes() []byte {
	var bs bytes.Buffer
	binary.Write(&bs, binary.LittleEndian, h)
	return bs.Bytes()
}

func FromBytes(bs []byte) FileHeader {
	return FileHeader{
		PageSize:          binary.LittleEndian.Uint16(bs[16:18]),
		FileChangeCounter: binary.LittleEndian.Uint32(bs[24:28]),
		SchemaVersion:     binary.LittleEndian.Uint32(bs[40:44]),
		PageCacheSize:     binary.LittleEndian.Uint32(bs[48:52]),
		UserCookie:        binary.LittleEndian.Uint32(bs[60:64]),
	}
}

func NewPageHeader(t PageType) PageHeader {
	return PageHeader{
		Type: t,
	}
}

// PageType tye type of page. See
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

// PageHeader contains metadata about the page
type PageHeader struct {
	// Type is the PageType for the page
	Type PageType

	// FreeOffset is the byte offset at which the free space starts. Note that this must be updated every time the cell offset array grows.
	FreeOffset uint16

	// NumCells is the number of cells stored in this page.
	NumCells uint16

	// CellsOffset is the byte offset at which the cells start. If the page contains no cells, this field contains the value PageSize. This value must be updated every time a cell is added.
	CellsOffset uint16

	// Always zero for serialization
	xxZero uint8

	// RightPage internal nodes only
	RightPage uint32
}

type TablePage struct {
	Header PageHeader
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
	Key SQLType = iota
	Byte
	SmallInt
	Integer
	Text
)

type Record struct {
	Fields []Field
}

type Field struct {
	Type SQLType
	Data interface{}
}

func NewRecord(fields []Field) *Record {
	return &Record{
		Fields: fields,
	}
}

func (r *Record) ToBytes() []byte {
	var buf bytes.Buffer

	buf.WriteByte(0)
	for _, f := range r.Fields {
		// If data is nil indicate
		// the SQL type is NULL
		if f.Data == nil {
			buf.WriteByte(0)
			continue
		}

		switch f.Type {
		case Key:
			buf.WriteByte(0)
		case Byte:
			buf.WriteByte(1)
		case SmallInt:
			buf.WriteByte(2)
		case Integer:
			buf.WriteByte(4)
		case Text:
			fieldSize := uint32(2*len(f.Data.(string)) + 13)
			Put28BitInt(&buf, fieldSize)
		default:
			panic("Unknown sql type")
		}
	}

	// Capture the length of the header
	headerLen := uint8(buf.Len())

	for _, f := range r.Fields {
		// Nil is specified handled in header
		// Key is in Header
		if f.Data == nil || f.Type == Key {
			continue
		}

		switch f.Data.(type) {
		case int8:
			buf.WriteByte(byte(f.Data.(int8)))
		case byte:
			buf.WriteByte(f.Data.(byte))
		case int16:
			binary.Write(&buf, binary.BigEndian, uint16(f.Data.(int16)))
		case int32:
			binary.Write(&buf, binary.BigEndian, uint32(f.Data.(int32)))
		case int64:
			binary.Write(&buf, binary.BigEndian, uint32(f.Data.(int64)))
		case int:
			binary.Write(&buf, binary.BigEndian, uint32(f.Data.(int)))
		case string:
			buf.Write([]byte(f.Data.(string)))
		}
	}

	bs := buf.Bytes()
	bs[0] = headerLen
	return buf.Bytes()
}

// Put28BitInt writes a 28 bit integer into 4 bytes based on the encoding described here:
// http://chi.cs.uchicago.edu/chidb/fileformat.html
func Put28BitInt(w io.Writer, x uint32) {
	if x > 0xFFFFFFF {
		panic("only 28bits can be used")
	}

	buf := make([]byte, 4)

	for i := 3; i >= 0; i-- {
		// each byte encodes 7 bits plus the MSB = 1 or 0 for LSB flag
		var flag byte = 0x80
		if i == 0 {
			flag = 0x0
		}

		// read the nth 7 bits
		var sevenBitInt byte = byte(0x7F & (x >> (i * 7)))
		buf[3-i] = flag | sevenBitInt
		// Encode windows of 7 bits
	}

	w.Write(buf)
}
