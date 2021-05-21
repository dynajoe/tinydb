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
	h := NewFileHeader(1024)

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
	h := NewFileHeader(1024)
	_, err := h.WriteTo(&buf)
	assert.NoError(err)

	bs := buf.Bytes()
	result, err := ParseFileHeader(bs)
	assert.NoError(err)
	assert.Equal(h, result)
}
