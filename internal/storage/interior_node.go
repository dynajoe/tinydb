package storage

import (
	"bytes"
	"encoding/binary"
	"io"
)

const InteriorNodeSize int = 8

type InteriorNode struct {
	LeftChild uint32
	Key       uint32
}

// ToBytes serializes an interior node to a byte slice
func (r InteriorNode) ToBytes() ([]byte, error) {
	buf := bytes.Buffer{}
	if err := r.Write(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Write writes an interior node to the specified writer
func (r InteriorNode) Write(bs io.ByteWriter) error {
	recordBuffer := bytes.Buffer{}

	// Write the child page
	if err := binary.Write(&recordBuffer, binary.BigEndian, r.LeftChild); err != nil {
		return err
	}

	// Write the key
	WriteVarint(&recordBuffer, uint64(r.Key))

	// Write to the byte writer
	// TODO: this seems ineffective.
	for _, b := range recordBuffer.Bytes() {
		if err := bs.WriteByte(b); err != nil {
			return err
		}
	}

	return nil
}

// ReadInteriorNode parses an interior node from a byte slice
func ReadInteriorNode(data []byte) (*InteriorNode, error) {
	reader := bytes.NewReader(data)

	var leftChild uint32
	if err := binary.Read(reader, binary.BigEndian, &leftChild); err != nil {
		return nil, err
	}

	key, _, err := ReadVarint(reader)
	if err != nil {
		return nil, err
	}

	return &InteriorNode{LeftChild: leftChild, Key: uint32(key)}, nil
}
