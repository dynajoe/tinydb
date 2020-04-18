package engine

import (
	"github.com/joeandaverde/tinydb/internal/storage"
)

type op uint8

// https://github.com/uchicago-cs/chidb/blob/master/src/libchidb/dbm-types.h
const (
	OP_NoOp op = iota
	OP_OpenRead
	OP_OpenWrite
	OP_Close
	OP_Rewind
	OP_Next
	OP_Prev
	OP_Seek
	OP_SeekGt
	OP_SeekGe
	OP_SeekLt
	OP_SeekLe
	OP_Column
	OP_Key
	OP_Integer
	OP_String
	OP_Null
	OP_ResultRow
	OP_MakeRecord
	OP_Insert
	OP_Eq
	OP_Ne
	OP_Lt
	OP_Le
	OP_Gt
	OP_Ge
	OP_IdxGt
	OP_IdxGe
	OP_IdxLt
	OP_IdxLe
	OP_IdxPKey
	OP_IdxInsert
	OP_CreateTable
	OP_CreateIndex
	OP_Copy
	OP_SCopy
	OP_Halt
)

type reg uint

const (
	REG_UNSPECIFIED = 0
	REG_NULL        = 1
	REG_INT32       = 2
	REG_STRING      = 3
	REG_BINARY      = 4
)

type register struct {
	typ  reg
	data []byte
}

type program struct {
	engine  *Engine
	pc      int
	ops     []op
	regs    []register
	cursors []*storage.Cursor
}

func (p *program) Run() {
	p.pc = 0
	p.regs = make([]register, 5)

	for p.pc < len(p.ops) {
		nextPc := p.step()
		if nextPc > 0 {
			p.pc = nextPc
			continue
		}
		p.pc = p.pc + 1
	}
}

func (p *program) step() int {
	switch p.ops[p.pc] {
	case OP_NoOp:

	case OP_OpenRead:
		cursor := int(p.regs[0].data[0])
		pageNo := int(p.regs[1].data[0])
		// cols := regs[2].data[0]
		f, err := p.engine.Pager.OpenRead(pageNo)
		if err != nil {
			panic("open read error")
		}
		p.cursors[cursor] = f
	case OP_OpenWrite:
		cursor := int(p.regs[0].data[0])
		pageNo := int(p.regs[1].data[0])
		// cols := regs[2].data[0]
		f, err := p.engine.Pager.OpenWrite(pageNo)
		if err != nil {
			panic("open write error")
		}
		p.cursors[cursor] = f
	case OP_Close:
		cursor := p.cursors[int(p.regs[0].data[0])]
		p.engine.Pager.CloseCursor(cursor)
	case OP_Rewind:
		cursor := p.cursors[int(p.regs[0].data[0])]
		p.engine.Pager.CloseCursor(cursor)
	}

	return 0
}
