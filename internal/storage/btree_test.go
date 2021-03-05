package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInteriorNode_ToBytes(t *testing.T) {
	assert := require.New(t)

	cell := InteriorNode{
		LeftChild: uint32(2),
		Key:       999,
	}

	cellBytes, err := cell.ToBytes()
	assert.NoError(err)

	assert.Equal([]byte{0x0, 0x0, 0x0, 0x2, 0xa}, cellBytes)
}
