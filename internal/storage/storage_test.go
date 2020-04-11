package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileHeader_ToBytes(t *testing.T) {
	assert := require.New(t)
	h := NewFileHeader()
	bs := h.ToBytes()
	assert.Equal([]byte{'S', 'Q', 'L', 'i', 't', 'e', ' ', 'f', 'o', 'r', 'm', 'a', 't', ' ', '3', 0}, bs[:16])
	assert.Equal([]byte{0x64, 0x0}, bs[16:18])
}

func TestFileHeader_FromBytes(t *testing.T) {
	assert := require.New(t)
	h := NewFileHeader()
	bs := h.ToBytes()
	result := FromBytes(bs)
	assert.Equal(h, result)
}

func TestRecord_ToBytes(t *testing.T) {
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
	bs := h.ToBytes()
	assert.Equal([]byte{
		0x08, // Length
		0x00, // Primary Key (Always NULL)
		0x80, // MSB Text
		0x80, // ..
		0x80, // ..
		0x1F, // LSB Text
		0x00, // NULL
		0x04, // Integer
		'D', 'a', 't', 'a', 'b', 'a', 's', 'e', 's',
		0x0, 0x0, 0x0, 0x2A, // 42
	}, bs)
}
