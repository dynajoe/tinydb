package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"io"
	"math/rand"
	"os"
	"sync"
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
	dbFile           *DbFile
	checkpointNumber uint32
	salt1            uint32
	salt2            uint32
	pos              uint32
	totalPages       int

	pageCache map[int][]byte
	mu        *sync.RWMutex
}

func OpenWAL(dbFile *DbFile) (*WAL, error) {
	f, err := os.OpenFile(dbFile.Path()+"-wal", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:       f,
		dbFile:     dbFile,
		mu:         &sync.RWMutex{},
		totalPages: dbFile.TotalPages(),
		pageCache:  make(map[int][]byte),
	}, nil
}

func (w *WAL) writeLog(pageNumber int, data []byte, isCommit bool) error {
	frame, err := w.makeWalFrame(pageNumber, data, isCommit)
	if err != nil {
		return err
	}

	if _, err := io.Copy(w.file, bytes.NewReader(frame)); err != nil {
		return err
	} else if err := w.file.Sync(); err != nil {
		return err
	}

	w.pos += uint32(len(frame))
	return nil
}

func (w *WAL) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write all pages to db file
	var pagesToWrite []Page
	for pageNumber, data := range w.pageCache {
		pagesToWrite = append(pagesToWrite, Page{PageNumber: pageNumber, Data: data})
	}

	if len(pagesToWrite) > 0 {
		if err := w.dbFile.Write(pagesToWrite...); err != nil {
			return err
		}
	}

	// Checkpoints always start at the beginning of the file
	w.pos = 0

	return nil
}

func (w *WAL) writeHeader() error {
	header := make([]byte, WALHeaderLen)

	w.checkpointNumber++
	w.salt1 = rand.Uint32()
	w.salt2 = rand.Uint32()

	binary.BigEndian.PutUint32(header[0:4], WALMagicNumber)
	binary.BigEndian.PutUint32(header[4:8], WALFileFormat)
	binary.BigEndian.PutUint32(header[8:12], uint32(w.dbFile.PageSize()))
	binary.BigEndian.PutUint32(header[12:16], w.checkpointNumber)
	binary.BigEndian.PutUint32(header[16:20], w.salt1)
	binary.BigEndian.PutUint32(header[20:24], w.salt2)

	// Calculate the sum of the header up to this point
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	_, err := h.Write(header[:24])
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(header[24:32], h.Sum64())

	// Write the header to the start of the file & flush
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	} else if _, err := io.Copy(w.file, bytes.NewReader(header)); err != nil {
		return err
	} else if err = w.file.Sync(); err != nil {
		return err
	}

	// The next write to the WAL will be here.
	w.pos = WALHeaderLen

	return nil
}

func (w *WAL) makeWalFrame(pageNumber int, data []byte, isCommit bool) ([]byte, error) {
	header := make([]byte, WALFrameHeaderLen, WALFrameHeaderLen+w.dbFile.PageSize())

	binary.BigEndian.PutUint32(header[0:4], uint32(pageNumber))

	if isCommit {
		binary.BigEndian.PutUint32(header[4:8], 1)
	} else {
		binary.BigEndian.PutUint32(header[4:8], 0)
	}

	binary.BigEndian.PutUint32(header[8:12], w.salt1)
	binary.BigEndian.PutUint32(header[12:16], w.salt2)

	// The checksum values in the final 8 bytes of the frame-header exactly
	// match the checksum computed consecutively on the first 24 bytes of
	// the WAL header and the first 8 bytes and the content of all frames
	// up to and including the current frame.
	binary.BigEndian.PutUint64(header[24:32], 0)

	pageBuffer := bytes.NewBuffer(header)
	if _, err := pageBuffer.Write(header); err != nil {
		return nil, err
	} else if _, err := pageBuffer.Write(data); err != nil {
		return nil, err
	}

	return pageBuffer.Bytes(), nil
}

func (w *WAL) PageSize() int {
	return w.dbFile.PageSize()
}

func (w *WAL) TotalPages() int {
	return w.totalPages
}

func (w *WAL) Read(page int) ([]byte, error) {
	if data, ok := w.pageCache[page]; ok {
		dest := make([]byte, len(data))
		copy(dest, data)
		return dest, nil
	}
	return w.dbFile.Read(page)
}

func (w *WAL) Write(pages ...Page) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// First page in the wal
	if w.pos == 0 {
		if err := w.writeHeader(); err != nil {
			return err
		}
	}

	// Write all pages out. The last page written is the commit page.
	for i, p := range pages {
		w.pageCache[p.PageNumber] = p.Data
		if p.PageNumber > w.totalPages {
			w.totalPages = p.PageNumber
		}

		lastPage := i == len(pages)-1
		if err := w.writeLog(p.PageNumber, p.Data, lastPage); err != nil {
			return err
		}
	}

	return nil
}

// checkSum only works for content which is an odd multiple of 8 bytes in length.
func checkSum(b []byte, s0, s1 uint32, order binary.ByteOrder) (uint32, uint32, error) {
	// Work in chunks of 8 bytes, x better be odd
	x := len(b) >> 3
	if x%2 == 0 {
		return 0, 0, errors.New("checkSum only works with odd multiples of 8 bytes")
	}

	for i := 0; i < x; i++ {
		offset := i * 8
		s0 += order.Uint32(b[offset:]) + s1
		s1 += order.Uint32(b[offset+4:]) + s0
	}
	return s0, s1, nil
}

var _ PageReader = (*WAL)(nil)
var _ PageWriter = (*WAL)(nil)
