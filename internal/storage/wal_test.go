package storage

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChecksum(t *testing.T) {
	assert := require.New(t)
	header := []byte{
		0x37, 0x7F, 0x06, 0x82, // 0
		0x00, 0x2D, 0xE2, 0x18, // 4

		0x00, 0x00, 0x10, 0x00, // 8
		0x00, 0x00, 0x00, 0x00, // 12

		0x96, 0x79, 0x22, 0x93, // 16
		0xA8, 0xB8, 0xFF, 0xA6, // 20
	}
	var expectedSum1 uint32 = 0x68097CA9
	var expectedSum2 uint32 = 0xC6F10CF6

	s0, s1 := checkSum(header, 0, 0, binary.LittleEndian)

	assert.Equal(expectedSum1, s0)
	assert.Equal(expectedSum2, s1)
}
