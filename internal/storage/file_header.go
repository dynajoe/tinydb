package storage

import (
	"encoding/binary"
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
	// Size in pages of the database
	SizeInPages uint32
}

// NewFileHeader creates a new FileHeader
func NewFileHeader(pageSize uint16) FileHeader {
	return FileHeader{
		PageSize:          pageSize,
		FileChangeCounter: 0,
		SchemaVersion:     0,
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
	binary.BigEndian.PutUint32(data[44:], 4) // Schema format
	binary.BigEndian.PutUint32(data[48:], 0)
	binary.BigEndian.PutUint32(data[52:], 0)
	binary.BigEndian.PutUint32(data[56:], 1)
	binary.BigEndian.PutUint32(data[64:], 0)
	binary.BigEndian.PutUint32(data[92:], 3)
	binary.BigEndian.PutUint32(data[96:], 3027002)
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
	}
}
