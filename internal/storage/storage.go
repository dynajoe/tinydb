package storage

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/joeandaverde/tinydb/internal/btree"
	"golang.org/x/net/context"
)

func RowReader(p *MemPage) <-chan Record {
	rowChan := make(chan Record)
	go func() {
		// TODO: assumes always leaf page
		offset := 8
		if p.PageNumber == 1 {
			offset = offset + 100
		}

		// TODO: Should have a configuration read timeout
		readTimeout, _ := context.WithTimeout(context.Background(), time.Second*5)

		// TODO: are there gaps in the list?
		reader := bytes.NewReader(p.Data[offset : uint16(offset)+p.NumCells*2])
		for i := 0; i < int(p.NumCells); i++ {
			var offset uint16
			if err := binary.Read(reader, binary.BigEndian, &offset); err != nil {
				panic(err.Error())
			}
			reader := bytes.NewReader(p.Data[offset:])
			record, err := ReadRecord(reader)
			if err != nil {
				panic(err.Error())
			}

			select {
			case rowChan <- record:
			case <-readTimeout.Done():
				break
			}
		}
		close(rowChan)
	}()

	return rowChan

}

func BTreeFromPage(p *MemPage) *btree.BTree {
	bt := btree.New(5)

	rowChan := RowReader(p)
	for record := range rowChan {
		bt.Insert(&btree.StringItem{
			Key:  record.Fields[1].Data.(string),
			Data: &record,
		})
	}

	return bt
}
