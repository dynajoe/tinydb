package engine

import (
	"encoding/binary"

	"github.com/joeandaverde/tinydb/internal/storage"
)

type op uint8

// https://github.com/uchicago-cs/chidb/blob/master/src/libchidb/dbm-types.h
const (
	OpNoOp op = iota
	OpOpenRead
	OpOpenWrite
	OpClose
	OpRewind
	OpNext
	OpPrev
	OpSeek
	OpSeekGt
	OpSeekGe
	OpSeekLt
	OpSeekLe
	OpColumn
	OpKey
	OpInteger
	OpString
	OpNull
	OpResultRow
	OpMakeRecord
	OpInsert
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpIdxGt
	OpIdxGe
	OpIdxLt
	OpIdxLe
	OpIdxPKey
	OpIdxInsert
	OpCreateTable
	OpCreateIndex
	OpCopy
	OpSCopy
	OpHalt
)

type reg uint

const (
	RegUnspecified = 0
	RegNull        = 1
	RegInt32       = 2
	RegString      = 3
	RegBinary      = 4
)

type register struct {
	typ  reg
	data []byte
}

type instruction struct {
	op op
	p1 int
	p2 int
	p3 int
	p4 int
}

type program struct {
	engine       *Engine
	pc           int
	instructions []instruction
	regs         []*register
	cursors      []*storage.Cursor
	strings      []string
	halted       bool
	results      chan []interface{}
}

func NewProgram(e *Engine, i []instruction, s []string) *program {
	regs := make([]*register, 5)
	for i := range regs {
		regs[i] = &register{
			typ:  RegUnspecified,
			data: nil,
		}
	}

	return &program{
		cursors:      nil,
		engine:       e,
		instructions: i,
		pc:           0,
		strings:      s,
		regs:         regs,
		results:      make(chan []interface{}),
	}
}

func (p *program) Run() {
	defer close(p.results)

	for p.pc < len(p.instructions) {
		nextPc := p.step()
		if p.halted {
			return
		}
		if nextPc > 0 {
			p.pc = nextPc
			continue
		}
		p.pc = p.pc + 1
	}
}

func (p *program) step() int {
	i := p.instructions[p.pc]

	switch i.op {
	case OpNoOp:
	case OpHalt:
		p.halted = true
	case OpInteger:
		v := i.p1
		r := i.p2
		reg := p.regs[r]

		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, uint32(v))
		reg.data = data
		reg.typ = RegInt32
	case OpString:
		r := i.p2
		s := p.strings[i.p3]
		reg := p.regs[r]
		reg.data = []byte(s)
		reg.typ = RegString
	case OpNull:
		r := i.p2
		reg := p.regs[r]
		reg.data = nil
		reg.typ = RegNull
	case OpSCopy:
		r1 := i.p1
		r2 := i.p2
		p.regs[r2] = p.regs[r1]
	case OpEq:
		a := p.regs[i.p1]
		jmp := i.p2
		b := p.regs[i.p3]
		if eq(a, b) {
			return jmp
		}
	case OpLt:
		a := p.regs[i.p1]
		jmp := i.p2
		b := p.regs[i.p3]
		if less(a, b) {
			return jmp
		}
	case OpLe:
		a := p.regs[i.p1]
		jmp := i.p2
		b := p.regs[i.p3]
		if less(a, b) || eq(a, b) {
			return jmp
		}
	case OpGt:
		a := p.regs[i.p1]
		jmp := i.p2
		b := p.regs[i.p3]
		if !less(a, b) && !eq(a, b) {
			return jmp
		}
	case OpGe:
		a := p.regs[i.p1]
		jmp := i.p2
		b := p.regs[i.p3]
		if !less(a, b) {
			return jmp
		}
	case OpNe:
		a := p.regs[i.p1]
		jmp := i.p2
		b := p.regs[i.p3]
		if !eq(a, b) {
			return jmp
		}
	case OpOpenRead:
		cursor := i.p1
		pageNo := i.p2
		// cols := instruction.params[2]
		f, err := p.engine.Pager.OpenRead(pageNo)
		if err != nil {
			panic("open read error")
		}
		p.cursors[cursor] = f
	case OpOpenWrite:
		cursor := i.p1
		pageNo := i.p2
		// cols := instruction.params[2]
		f, err := p.engine.Pager.OpenWrite(pageNo)
		if err != nil {
			panic("open write error")
		}
		p.cursors[cursor] = f
	case OpClose:
		cursor := p.cursors[i.p1]
		p.engine.Pager.CloseCursor(cursor)
	case OpRewind:
		cursor := p.cursors[i.p1]
		if err := cursor.Rewind(); err != nil {
			panic("error while rewinding cursor")
		}
	case OpColumn:
		// cursor := p.cursors[i.p1]
		// col := i.p2
		// reg := p.regs[i.p3]
	case OpResultRow:
		startReg := i.p1
		colCount := i.p2
		endReg := startReg + colCount - 1
		var result []interface{}
		for i := startReg; i <= endReg; i++ {
			reg := p.regs[i]
			switch reg.typ {
			case RegInt32:
				result = append(result, int(binary.BigEndian.Uint32(reg.data)))
			case RegBinary:
				// TODO: shoud copy the buffer?
				result = append(result, reg.data)
			case RegString:
				result = append(result, string(reg.data))
			case RegNull:
				result = append(result, nil)
			}
		}
		p.results <- result
	}

	return 0
}

func less(a *register, b *register) bool {
	if a.typ != b.typ {
		return false
	}

	switch a.typ {
	case RegString:
		return string(a.data) < string(b.data)
	case RegInt32:
		return binary.BigEndian.Uint32(a.data) < binary.BigEndian.Uint32(b.data)
	case RegNull:
		return false
	case RegBinary:
		// Two binary blobs are equal if and only if they have the same length
		// and contain the exact same bytes. If two binary blobs have different
		// lengths, order is determined by the common bytes between the two blobs.
		// If the common bytes are equal, then the blob with the fewer bytes
		// is considered to be less than the blob with more bytes.
		// TODO: implement above
		return len(a.data) < len(b.data)
	}

	return false
}

func eq(a *register, b *register) bool {
	if len(a.data) != len(b.data) {
		return false
	}

	for i, v := range a.data {
		if v != b.data[i] {
			return false
		}
	}
	return true
}
