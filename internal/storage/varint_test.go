package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVarint32(t *testing.T) {
	r := require.New(t)

	for i := 0; i < 2048; i++ {
		bs := bytes.Buffer{}
		_, err := WriteVarint(&bs, uint64(i))
		r.NoError(err)

		reader := bytes.NewReader(bs.Bytes())
		v, _, err := ReadVarint(reader)
		r.NoError(err)

		r.Equal(uint64(i), uint64(v))
	}
}
