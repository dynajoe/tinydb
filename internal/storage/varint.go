package storage

import (
	"bytes"
	"io"
)

// ReadVarint reads a varint in little endian order.
func ReadVarint(reader io.ByteReader) (uint64, int, error) {
	// Copy varint bytes to buffer
	buf := bytes.Buffer{}
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, buf.Len(), err
		}
		buf.WriteByte(b)
		if b&0x80 == 0 {
			break
		}
	}

	// Reverse to reconstruct in big endian order
	z := buf.Bytes()
	for i, j := 0, len(z)-1; i < j; i, j = i+1, j-1 {
		z[i], z[j] = z[j], z[i]
	}

	// Reconstruct big endian order
	// TODO: it will depend on system
	var x uint64
	var s uint
	for _, b := range z {
		x |= uint64(b&0x7f) << s
		s += 7
	}

	return x, len(z), nil
}

// WriteVarint writes a varint in little endian order.
func WriteVarint(w io.ByteWriter, v uint64) (int, error) {
	// Collect bytes to be encoded
	buf := bytes.Buffer{}
	for {
		buf.WriteByte(byte(v & 0x7f))
		v >>= 7
		if v == 0 {
			break
		}
	}

	// Reverse to write in little endian order
	// TODO: it will depend on system
	s := buf.Bytes()
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}

	// Encode bytes
	for i, b := range s {
		if i < len(s)-1 {
			w.WriteByte(b | 0x80)
		} else {
			w.WriteByte(b & 0x7f)
		}
	}

	return buf.Len(), nil
}
