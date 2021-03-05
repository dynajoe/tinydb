package storage

import (
	"bytes"
	"encoding/binary"
	"time"

	"golang.org/x/net/context"
)

var readTimeoutSeconds int = -1

type Payload struct {
	Err    error
	Record *Record
}

func RowReader(p *MemPage) <-chan Payload {
	rowChan := make(chan Payload)
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
				rowChan <- Payload{Record: nil, Err: err}
				break
			}

			reader := bytes.NewReader(p.Data[offset:])
			record, err := ReadRecord(reader)
			if err != nil {
				rowChan <- Payload{Record: nil, Err: err}
				break
			}

			ctx := context.Background()
			if readTimeoutSeconds > 0 {
				ctx, _ = context.WithTimeout(ctx, time.Duration(readTimeoutSeconds)*time.Second)
			}

			select {
			case rowChan <- Payload{Record: record}:
			case <-ctx.Done():
				break
			}
		}
		close(rowChan)
	}()

	return rowChan
}
