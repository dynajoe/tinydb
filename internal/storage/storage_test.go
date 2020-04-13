package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileHeader_Write(t *testing.T) {
	assert := require.New(t)

	buf := bytes.Buffer{}
	h := NewFileHeader()

	// WriteTo the contents to Writer
	_, err := h.WriteTo(&buf)
	assert.NoError(err)

	// Assert
	bs := buf.Bytes()
	assert.Equal([]byte{'S', 'Q', 'L', 'i', 't', 'e', ' ', 'f', 'o', 'r', 'm', 'a', 't', ' ', '3', 0}, bs[:16])
	assert.Equal(h.PageSize, binary.BigEndian.Uint16(bs[16:18]))
	assert.Len(bs, 100)
}

func TestFileHeader_ReadHeader(t *testing.T) {
	assert := require.New(t)
	buf := bytes.Buffer{}
	h := NewFileHeader()
	_, err := h.WriteTo(&buf)
	assert.NoError(err)

	bs := buf.Bytes()
	result := ParseFileHeader(bs)
	assert.Equal(h, result)
}

func TestRecord_Write(t *testing.T) {
	assert := require.New(t)
	stringContent := "Databases"
	h := NewRecord([]Field{
		Field{
			Type: Key,
			Data: 23500,
		},
		Field{
			Type: Text,
			Data: stringContent,
		},
		Field{
			Type: Integer,
			Data: nil,
		},
		Field{
			Type: Integer,
			Data: int(42),
		},
	})

	buf := bytes.Buffer{}
	err := h.Write(&buf)
	assert.NoError(err)

	expectedPrefix := []byte{
		// Header Size (includes first byte)
		5,
		// Primary Key (Always NULL)
		0,
		// Text
		0x3E,
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

func TestWriteRecord(t *testing.T) {
	assert := require.New(t)

	page := NewPage(2, 256)
	record := NewRecord([]Field{
		{
			Type: Integer,
			Data: 1337,
		},
	})

	err := WriteRecord(page, record)
	assert.NoError(err)
	// 250 = (256 bytes - 6 bytes)
	assert.Equal([]byte{0x00, 0xFA}, page.Data[0:2])
	// varint - length of header
	assert.Equal(byte(2), page.Data[250])
	// Integer type (1 byte)
	assert.Equal(byte(Integer), page.Data[251])
	// Data, 4 byte big endian integer
	assert.Equal([]byte{0, 0, 0x05, 0x39}, page.Data[252:])
}

func TestWriteRecord_WithText(t *testing.T) {
	assert := require.New(t)

	expectedText := "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et"
	pageSize := 256
	page := NewPage(2, uint16(pageSize))
	record := NewRecord([]Field{
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

	// The header should encode the length of the text as a varint
	// Lorem = (len(lorem) * 2 + 13) = 217 = 2 (7 bit bytes)
	// Size = 1 + 1 (int) + 2 (text) = 4
	expectedHeader := []byte{0x4, 0x4, 0xB2, 0x3}
	actualHeader := page.Data[page.CellsOffset : int(page.CellsOffset)+len(expectedHeader)]
	assert.Equal(expectedHeader, actualHeader)

	// The text should be at the end of data
	assert.Equal([]byte(expectedText), page.Data[pageSize-len(expectedText):])
}
