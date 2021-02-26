package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"
	"io"
	"math/rand"
	"os"
)

const (
	WALHeaderLen = 32

	WALFrameHeaderLen = 24

	WALMagicNumber = 0x377f0682

	WALFileFormat = 3007000
)

// WAL Header Format
// Offset	Size	Description
// 0			4	Magic number. 0x377f0682 or 0x377f0683
// 4			4	File format version. Currently 3007000.
// 8			4	Database page size. Example: 1024
// 12			4	Checkpoint sequence number
// 16			4	Salt-1: random integer incremented with each checkpoint
// 20			4	Salt-2: a different random number for each checkpoint
// 24			4	Checksum-1: First part of a checksum on the first 24 bytes of header
// 28			4	Checksum-2: Second part of the checksum on the first 24 bytes of header

// WAL Frame Header Format
// Offset	Size	Description
// 0			4	Page number
// 4			4	For commit records, the size of the database file in pages after the commit. For all other records, zero.
// 8			4	Salt-1 copied from the WAL header
// 12			4	Salt-2 copied from the WAL header
// 16			4	Checksum-1: Cumulative checksum up through and including this page
// 20			4	Checksum-2: Second half of the cumulative checksum.

// WAL represents a write ahead log
type WAL struct {
	file             *os.File
	pageSize         uint32
	checkpointNumber uint32
}

func (w *WAL) Init(path string, pageSize uint32) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	w.file = f
	w.pageSize = pageSize

	// Add header
	if err := w.appendHeader(); err != nil {
		return err
	}

	return nil
}

func (w *WAL) appendHeader() error {
	header := make([]byte, WALHeaderLen)

	binary.BigEndian.PutUint32(header[0:4], WALMagicNumber)
	binary.BigEndian.PutUint32(header[4:8], WALFileFormat)
	binary.BigEndian.PutUint32(header[8:12], w.pageSize)
	binary.BigEndian.PutUint32(header[12:16], w.checkpointNumber)
	binary.BigEndian.PutUint32(header[16:20], rand.Uint32())
	binary.BigEndian.PutUint32(header[20:24], rand.Uint32())

	// Calculate the sum of the header up to this point
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	_, err := h.Write(header[:24])
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(header[24:32], h.Sum64())

	// Write the header & flush
	if _, err := io.Copy(w.file, bytes.NewReader(header)); err != nil {
		return err
	} else if err = w.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (w *WAL) appendFrameHeader(pageNumber uint32, salt1, salt2 uint32) error {
	header := make([]byte, WALFrameHeaderLen)

	binary.BigEndian.PutUint32(header[0:4], pageNumber)
	binary.BigEndian.PutUint32(header[4:8], 0)
	binary.BigEndian.PutUint32(header[8:12], salt1)
	binary.BigEndian.PutUint32(header[12:16], salt2)

	// The checksum values in the final 8 bytes of the frame-header exactly
	// match the checksum computed consecutively on the first 24 bytes of
	// the WAL header and the first 8 bytes and the content of all frames
	// up to and including the current frame.
	binary.BigEndian.PutUint64(header[24:32], 0)

	// Write the frame header & flush
	if _, err := io.Copy(w.file, bytes.NewReader(header)); err != nil {
		return err
	} else if err = w.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (w *WAL) Write() {}

func (w *WAL) Checkpoint() error {
	// TODO: Flush everything to db file

	// Checkpoints always start at the beginning of the file
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	// Start a new checkpoint
	w.checkpointNumber++
	if err := w.appendHeader(); err != nil {
		return err
	}

	return nil
}
