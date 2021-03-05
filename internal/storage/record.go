package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
)

type SQLType uint32

const (
	Null    = 0
	Byte    = 1
	Integer = 4
	Text    = 28
)

func SQLTypeFromString(t string) SQLType {
	switch t {
	case "text":
		return Text
	case "int":
		return Integer
	case "byte":
		return Byte
	}
	panic("unexpected SQL string type")
}

// Field is a field in a database record
type Field struct {
	Type SQLType
	Data interface{}
	Len  int
}

// Record is a set of fields
type Record struct {
	RowID  uint32
	Fields []*Field
}

// NewRecord creates a database record from a set of fields
func NewRecord(key uint32, fields []*Field) *Record {
	return &Record{
		RowID:  key,
		Fields: fields,
	}
}

func (r Record) ToBytes() ([]byte, error) {
	buf := bytes.Buffer{}
	if err := r.Write(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Write writes a record to the specified writer
func (r Record) Write(bs io.ByteWriter) error {
	// Record field types
	colBuf := bytes.Buffer{}
	for _, f := range r.Fields {
		// If data is nil indicate
		// the SQL type is NULL
		if f.Data == nil {
			colBuf.WriteByte(0)
			continue
		}

		switch f.Type {
		case Byte:
			colBuf.WriteByte(1)
		case Integer:
			colBuf.WriteByte(4)
		case Text:
			fieldSize := uint64(2*len(f.Data.(string)) + 13)
			_, err := WriteVarint(&colBuf, fieldSize)
			if err != nil {
				panic("unable to write varint")
			}
		default:
			panic("Unknown sql type")
		}
	}

	recordBuffer := bytes.Buffer{}

	// Record header Size
	// TODO: this assumes the header size can fit into 7 bit integer, which is usually okay.
	// Add 1 because to include the first byte that includes size
	recordBuffer.Write([]byte{byte(colBuf.Len() + 1)})

	// Copy field types to record buffer
	recordBuffer.Write(colBuf.Bytes())

	// Write data to record
	for _, f := range r.Fields {
		// Nil is specified handled in header
		if f.Data == nil {
			continue
		}

		switch f.Data.(type) {
		case int8:
			recordBuffer.Write([]byte{byte(f.Data.(int8))})
		case byte:
			recordBuffer.Write([]byte{f.Data.(byte)})
		case int:
			if err := binary.Write(&recordBuffer, binary.BigEndian, uint32(f.Data.(int))); err != nil {
				return err
			}
		case string:
			recordBuffer.Write([]byte(f.Data.(string)))
		default:
			return fmt.Errorf("not supported type: %v", reflect.TypeOf(f.Data))
		}
	}

	// Finally, write everything to the supplied writer
	// [Size, Key, Record]
	if _, err := WriteVarint(bs, uint64(len(recordBuffer.Bytes()))); err != nil {
		return err
	}
	if _, err := WriteVarint(bs, uint64(r.RowID)); err != nil {
		return err
	}
	for _, b := range recordBuffer.Bytes() {
		if err := bs.WriteByte(b); err != nil {
			return err
		}
	}

	return nil
}

// WriteInteriorEntry writes B-Tree Interior Cell (header 0x05):
// A 4-byte big-endian page number which is the left child pointer.
// A varint which is the integer key
func WriteInteriorEntry(p *MemPage, key int, leftChild uint32) {
	binary.BigEndian.PutUint32(p.Data[0:4], leftChild)
	buf := bytes.Buffer{}
	WriteVarint(&buf, uint64(key))
	copy(p.Data[4:], buf.Bytes())
}

// Header (12 bytes):
// 	pageType: 05
//     00 00
//     00 01
//     0F FB
//     00
//     00 00 00 04 (right page)
// left child: 0F FB 00 00
// interior page key: 00
// last bytes 03 07

func WriteRecord(p *MemPage, r *Record) error {
	buf := bytes.Buffer{}
	if err := r.Write(&buf); err != nil {
		return err
	}

	recordBytes := buf.Bytes()
	p.AddCell(recordBytes)

	return nil
}

func NewMasterTableRecord(rowID uint32, typeName string, name string, tableName string, rootPage int, sqlText string) *Record {
	return NewRecord(rowID, []*Field{
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

func ReadRecord(r io.ByteReader) (*Record, error) {
	_, _, err := ReadVarint(r)
	if err != nil {
		return nil, err
	}

	rowID, _, err := ReadVarint(r)
	if err != nil {
		return nil, err
	}

	var fields []*Field
	recordHeaderLen, _, err := ReadVarint(r)
	// Subtract the # of bytes for the header len.
	// Need to find out how many bytes were used for the varint
	recordHeaderLen = recordHeaderLen - 1
	for recordHeaderLen > 0 {
		colType, n, err := ReadVarint(r)
		if err != nil {
			return nil, err
		}

		var sqlType SQLType
		numBytes := 1
		switch colType {
		case 0:
			// NULL
		case 1:
			sqlType = Byte
			numBytes = 1
		case 4:
			sqlType = Integer
			numBytes = 4
		default:
			// TODO: default for text isnt appropriate, it should be something like
			// odd numbers greater than 12?
			sqlType = Text
			numBytes = int(colType-13) / 2
		}

		fields = append(fields, &Field{
			Type: sqlType,
			Len:  numBytes,
			Data: nil,
		})

		recordHeaderLen = recordHeaderLen - uint64(n)
	}

	for _, f := range fields {
		switch f.Type {
		case Byte:
			b, _ := r.ReadByte()
			f.Data = b
		case Integer:
			var bs []byte
			for i := 0; i < f.Len; i++ {
				b, _ := r.ReadByte()
				bs = append(bs, b)
			}
			f.Data = int(binary.BigEndian.Uint32(bs))
		case Text:
			var bs []byte
			for i := 0; i < f.Len; i++ {
				b, _ := r.ReadByte()
				bs = append(bs, b)
			}
			f.Data = string(bs)
		}
	}

	return &Record{
		RowID:  uint32(rowID),
		Fields: fields,
	}, nil
}
