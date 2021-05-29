package pager

import (
	"context"
	"time"
)

type Pool struct {
	pager             Pager
	writeSema         chan int
	writeConnectionID int
}

func NewPool(p Pager) *Pool {
	return &Pool{
		pager:             p,
		writeSema:         make(chan int, 1),
		writeConnectionID: 0,
	}
}

// Acquire provides a pager with the following constraints:
// any number of reader pagers that can only read data committed previously
// only one writer pager, blocks reader pagers from being acquired
// Blocks until the constraints are met and a pager can be acquired
func (p *Pool) Acquire(connectionID int) (Pager, error) {
	// block until writer is available or timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p.writeSema <- connectionID:
		p.writeConnectionID = connectionID
		return p.pager, nil
	}
}

func (p *Pool) Release(connectionID int) {
	if connectionID == p.writeConnectionID {
		p.writeConnectionID = 0
		<-p.writeSema
	}
}
