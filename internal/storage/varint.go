package storage

import "io"

func ReadVarint(reader io.ByteReader) (uint64, int, error) {
	return 0, 0, nil
}

func WriteVarint(w io.ByteWriter) (int, error) {
	return 0, nil
}
