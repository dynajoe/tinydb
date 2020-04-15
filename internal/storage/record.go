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
	Key    int
}

// NewRecord creates a database record from a set of fields
func NewRecord(key int, fields []Field) Record {
	return Record{
		Key:    key,
		Fields: fields,
	}
}

// WriteTo writes a record to the specified writer
func (r *Record) Write(bs io.Writer) error {
	// Build the header [varint header size..., cols...]
	var colBuf bytes.Buffer
	varintBuf := make([]byte, 9)
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
			fieldSize := uint64(2*len(f.Data.(string)) + 13)
			n := binary.PutUvarint(varintBuf, fieldSize)
			colBuf.Write(varintBuf[:n])
		default:
			panic("Unknown sql type")
		}
	}

	recordBuffer := bytes.Buffer{}
	// Size
	// TODO: this assumes the header size can fit into 7 bit integer, which is usually okay.
	// Add 1 because to include the first byte that includes size
	recordBuffer.Write([]byte{byte(colBuf.Len() + 1)})

	// Columns
	recordBuffer.Write(colBuf.Bytes())

	for _, f := range r.Fields {
		// Nil is specified handled in header
		// Key is in Header
		if f.Data == nil || f.Type == Key {
			continue
		}

		switch f.Data.(type) {
		case int8:
			recordBuffer.Write([]byte{byte(f.Data.(int8))})
		case byte:
			recordBuffer.Write([]byte{f.Data.(byte)})
		case int16:
			binary.Write(&recordBuffer, binary.BigEndian, uint16(f.Data.(int16)))
		case int32:
			binary.Write(&recordBuffer, binary.BigEndian, uint32(f.Data.(int32)))
		case int64:
			binary.Write(&recordBuffer, binary.BigEndian, uint32(f.Data.(int64)))
		case int:
			binary.Write(&recordBuffer, binary.BigEndian, uint32(f.Data.(int)))
		case string:
			recordBuffer.Write([]byte(f.Data.(string)))
		}
	}

	n := binary.PutUvarint(varintBuf, uint64(len(recordBuffer.Bytes())))
	bs.Write(varintBuf[:n])
	n = binary.PutUvarint(varintBuf, uint64(r.Key))
	bs.Write(varintBuf[:n])
	bs.Write(recordBuffer.Bytes())

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

func NewMasterTableRecord(key int, typeName string, name string, tableName string, rootPage int, sqlText string) Record {
	return NewRecord(key, []Field{
		{
			Type: Text,
			// type: text
			Data: typeName,
		},
		{
			Type: Text,
			// name: text
			Data: name,
		},
		{
			Type: Text,
			// tablename: text
			Data: tableName,
		},
		{
			Type: Byte,
			// TODO: this seems to be optimized from int to a byte by sqlite for early pages
			// rootpage: integer
			Data: byte(rootPage),
		},
		{
			Type: Text,
			// sql: text
			Data: sqlText,
		},
	})
}
