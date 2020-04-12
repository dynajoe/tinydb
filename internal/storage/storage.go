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

type DatabaseFile struct {
	pager *Pager
}

func Open(path string) (*DatabaseFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	// The file was created. Write file header.
	if info.Size() == 0 {
		if err := initDataFile(file); err != nil {
			return nil, err
		}
	}

	pager := NewPager(file)

	return &DatabaseFile{
		pager: pager,
	}, nil
}

func initDataFile(file *os.File) error {
	header := NewFileHeader()
	if err := header.Write(file); err != nil {
		return err
	}

	// Add a page to file to store the schema table
	pager := NewPager(file)
	pager.Allocate()
	firstPage := TablePage{
		PageHeader: NewPageHeader(PageTypeLeaf),
		Data:       nil,
	}
	pager.Write(1, &firstPage)

	// fsync
	if err := file.Sync(); err != nil {
		return err
	}

	return nil
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
func NewPageHeader(t PageType) PageHeader {
	return PageHeader{
		Type: t,
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

// TablePage represents a raw table page, the data is all
type TablePage struct {
	PageHeader
	Data []byte
}

func (h TablePage) Write(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, h)
}

func ReadTablePage(reader io.Reader, pageSize uint16) (*TablePage, error) {
	page := make([]byte, pageSize)

	bytesRead, err := reader.Read(page)
	if err != nil {
		return nil, err
	}
	if bytesRead != int(pageSize) {
		return nil, errors.New("unexpected page size")
	}

	tablePage := TablePage{
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
	Key SQLType = iota
	Byte
	SmallInt
	Integer
	Text
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
func NewRecord(fields []Field) *Record {
	return &Record{
		Fields: fields,
	}
}

// ToBytes serializes a database record for storage
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
	return bs
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
