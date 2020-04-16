package storage

import (
	"bytes"
	"encoding/binary"

	"github.com/joeandaverde/tinydb/internal/btree"
)

func BTreeFromPage(p *MemPage) *btree.BTree {
	bt := btree.New(5)

	cellsList := make([]uint16, p.NumCells)
	cells := make([]Record, p.NumCells)
	// TODO: assumes always leaf page
	offset := 8
	if p.PageNumber == 1 {
		offset = offset + 100
	}

	// Build a list of all cell addresses
	// TODO: are there gaps in the list?
	reader := bytes.NewReader(p.Data[offset : uint16(offset)+p.NumCells*2])
	for i := 0; i < int(p.NumCells); i++ {
		var temp uint16
		if err := binary.Read(reader, binary.BigEndian, &temp); err != nil {
			panic(err.Error())
		}
		cellsList[i] = temp
	}

	// Load records
	for i, offset := range cellsList {
		reader := bytes.NewReader(p.Data[offset:])
		record, err := ReadRecord(reader)
		if err != nil {
			panic(err.Error())
		}
		cells[i] = record
	}

	return bt
}
