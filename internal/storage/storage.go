package storage

import (
	"bytes"
	"encoding/binary"
	"time"

	"golang.org/x/net/context"
)

var readTimeoutSeconds int = -1

func RowReader(p *MemPage) <-chan Record {
	rowChan := make(chan Record)
	go func() {
		// TODO: assumes always leaf page
		offset := 8
		if p.PageNumber == 1 {
			offset = offset + 100
		}

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

			ctx := context.Background()
			if readTimeoutSeconds > 0 {
				ctx, _ = context.WithTimeout(ctx, time.Duration(readTimeoutSeconds)*time.Second)
			}

			select {
			case rowChan <- record:
			case <-ctx.Done():
				break
			}
		}
		close(rowChan)
	}()

	return rowChan
}
