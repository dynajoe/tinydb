package storage

import (
	"bytes"
	"encoding/binary"
	"io"
)

type SQLType uint32

const (
	Byte     = 1
	SmallInt = 2
	Integer  = 4
	Key      = 24
	Text     = 28
)

// Field is a field in a database record
type Field struct {
	Type SQLType
	Data interface{}
}

// Record is a set of fields
type Record struct {
	Fields []Field
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

	recordBytes := buf.Bytes()
	cellOffset := p.CellsOffset - uint16(len(recordBytes))
	cellOffsetPointer := p.NumCells * 2
	if p.PageNumber == 1 {
		cellOffsetPointer = cellOffsetPointer + 108 // header size + offset
	}

	// Write the offset of the data cell
	binary.BigEndian.PutUint16(p.Data[cellOffsetPointer:], cellOffset)

	// Update the page data
	copy(p.Data[cellOffset:], recordBytes)

	// Update cells offset for the next page
	p.CellsOffset = cellOffset

	// Update number of cells in this page
	p.NumCells = p.NumCells + 1

	return nil
}
