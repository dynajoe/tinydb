package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecord_Write(t *testing.T) {
	assert := require.New(t)
	stringContent := "Databases"
	h := NewRecord(1, []*Field{
		{
			Type: Integer,
			Data: 23500,
		},
		{
			Type: Text,
			Data: stringContent,
		},
		{
			Type: Integer,
			Data: nil,
		},
		{
			Type: Integer,
			Data: int(42),
		},
	})

	buf := bytes.Buffer{}
	err := h.Write(&buf)
	assert.NoError(err)

	expectedPrefix := []byte{
		// Cell Header
		0x12, 0x1,
		// Header Size (includes first byte)
		5,
		// Primary Key (Always NULL)
		0,
		// Text
		0x1F,
		// NULL
		0,
		// Integer
		byte(Integer),
		// End of Header
		'D', 'a', 't', 'a', 'b', 'a', 's', 'e', 's',
		0x0, 0x0, 0x0, 0x2A, // 42
	}

	assert.Equal(expectedPrefix, buf.Bytes()[:len(expectedPrefix)])
}

func TestWriteRecord_WithText(t *testing.T) {
	assert := require.New(t)

	expectedText := "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et"
	pageSize := 256
	page := NewPage(2, uint16(pageSize))
	record := NewRecord(5, []*Field{
		{
			Type: Integer,
			Data: 1337,
		},
		{
			Type: Text,
			Data: expectedText,
		},
	})

	err := WriteRecord(page, record)
	assert.NoError(err)

	expectedCellBytes := []byte{
		0x6e,     // Varint cell length,
		0x5,      // Varint _rowid_
		0x4,      // RecordHeader[record size]
		0x4,      // RecordHeader[int type]
		0x0, 0x0, // RecordHeader[text type] -- value written below
		0x0, 0x0, 0x05, 0x39, // RecordData[1337]
	}
	binary.PutUvarint(expectedCellBytes[4:6], uint64(len(expectedText)*2+13)) // RecordHeader[text type]
	expectedCellBytes = append(expectedCellBytes, []byte(expectedText)...)    // RecordData[text]

	assert.Equal(expectedCellBytes, page.Data[page.CellsOffset:])
}

func TestNewMasterTableRecord(t *testing.T) {
	assert := require.New(t)

	expectedBytes := []byte{0x36, 0x01, 0x06, 0x17, 0x19, 0x19, 0x01, 0x49,
		0x74, 0x61, 0x62, 0x6C, 0x65, 0x70, 0x65, 0x72, 0x73, 0x6F, 0x6E, 0x70, 0x65, 0x72, 0x73, 0x6F,
		0x6E, 0x02, 0x43, 0x52, 0x45, 0x41, 0x54, 0x45, 0x20, 0x54, 0x41, 0x42, 0x4C, 0x45, 0x20, 0x70,
		0x65, 0x72, 0x73, 0x6F, 0x6E, 0x28, 0x6E, 0x61, 0x6D, 0x65, 0x20, 0x74, 0x65, 0x78, 0x74, 0x29}
	masterTableRecord := NewMasterTableRecord(
		1, "table", "person", "person", 2, "CREATE TABLE person(name text)")
	buf := bytes.Buffer{}
	err := masterTableRecord.Write(&buf)
	assert.NoError(err)
	assert.Equal(expectedBytes, buf.Bytes())
}
