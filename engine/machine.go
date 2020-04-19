package engine

import (
	"github.com/joeandaverde/tinydb/internal/storage"
)

type op uint8

// https://github.com/uchicago-cs/chidb/blob/master/src/libchidb/dbm-types.h
const (
	OpNoOp op = iota
	// Opens B-Tree Rooted at page n
	// and stores cursor in c
	// P1 - cursor (c)
	// P2 - page number (n)
	// P3 - col count (0 if opening index)
	OpOpenRead
	// Opens B-Tree Rooted at page n
	// and stores cursor in c
	// P1 - cursor (c)
	// P2 - page number (n)
	// P3 - col count (0 if opening index)
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
	// Stores int in register
	// P1 - the int
	// P2 - the register
	OpInteger
	OpString
	OpNull
	OpResultRow
	// P1 - register start
	// P2 - count of cols
	// P3 - store record in this register
	OpMakeRecord
	// P1 - page
	OpRowID
	// P1 - cursor
	// P2 - register containing the record
	// P3 - register with record key
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
	// Create a new B-Tree
	// P1 - register for root page
	OpCreateTable
	OpCreateIndex
	OpCopy
	OpSCopy
	OpHalt
)

type reg uint

const (
	RegUnspecified reg = iota
	RegNull
	RegInt32
	RegString
	RegBinary
	RegRecord
)

type register struct {
	typ  reg
	data interface{}
}

type instruction struct {
	op op
	p1 int
	p2 int
	p3 int
	p4 interface{}
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

func NewProgram(e *Engine, i []instruction) *program {
	// TODO: Make this resizable
	regs := make([]*register, 10)
	for i := range regs {
		regs[i] = &register{
			typ:  RegUnspecified,
			data: nil,
		}
	}

	return &program{
		cursors:      make([]*storage.Cursor, 5),
		engine:       e,
		instructions: i,
		pc:           0,
		regs:         regs,
		results:      make(chan []interface{}),
	}
}

func (p *program) Run() error {
	defer close(p.results)

	for p.pc < len(p.instructions) {
		nextPc := p.step()
		if p.halted {
			return nil
		}
		if nextPc > 0 {
			p.pc = nextPc
			continue
		}
		p.pc = p.pc + 1
	}

	return nil
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
		writeInt(p.regs[r], v)
	case OpString:
		r := i.p2
		s := i.p4.(string)
		reg := p.regs[r]
		reg.data = s
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
		cursorIndex := i.p1
		pageNo := p.regs[i.p2].data.(int)
		// cols := instruction.params[2]
		f, err := p.engine.Pager.OpenWrite(pageNo)
		if err != nil {
			panic("open write error")
		}
		p.cursors[cursorIndex] = f
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
				result = append(result, reg.data.(int))
			case RegBinary:
				// TODO: shoud copy the buffer?
				result = append(result, reg.data.([]byte))
			case RegString:
				result = append(result, reg.data.(string))
			case RegNull:
				result = append(result, nil)
			}
		}
		p.results <- result
	case OpCreateTable:
		// Allocate a page for the new table
		rootPage, err := p.engine.Pager.Allocate()
		if err != nil {
			panic("unable to allocate page for table")
		}
		writeInt(p.regs[i.p1], rootPage.PageNumber)
	case OpMakeRecord:
		startReg := i.p1
		colCount := i.p2
		endReg := startReg + colCount - 1
		destReg := p.regs[i.p3]
		var fields []*storage.Field

		for i := startReg; i <= endReg; i++ {
			reg := p.regs[i]
			switch reg.typ {
			case RegInt32:
				// TODO: this needs to be more sophisticated and handle signed ints appropriately
				value := reg.data.(int)

				// Can this number fit in a single byte?
				if 0xFF&value == value {
					fields = append(fields, &storage.Field{
						Type: storage.Byte,
						Data: byte(value),
					})
					continue
				}

				// Can't fit in a single byte - store as int
				fields = append(fields, &storage.Field{
					Type: storage.Integer,
					Data: value,
				})
			case RegString:
				fields = append(fields, &storage.Field{
					Type: storage.Text,
					Data: reg.data.(string),
				})
			case RegNull:
				fields = append(fields, &storage.Field{
					Type: storage.Null,
					Data: nil,
				})
			default:
				panic("unsupported register type for record")
			}
		}

		destReg.typ = RegRecord
		destReg.data = storage.NewRecord(fields)
	case OpRowID:
		writeInt(p.regs[i.p1], nextKey("master"))
	case OpInsert:
		cursor := p.cursors[i.p1]
		record := p.regs[i.p2].data.(storage.Record)
		key := p.regs[i.p3].data.(int)
		if err := cursor.Insert(key, record); err != nil {
			panic("error performing insert")
		}
	}

	return 0
}

func writeInt(reg *register, v int) {
	reg.data = v
	reg.typ = RegInt32
}

func less(a *register, b *register) bool {
	if a.typ != b.typ {
		return false
	}

	switch a.typ {
	case RegString:
		return a.data.(string) < b.data.(string)
	case RegInt32:
		return a.data.(int) < b.data.(int)
	case RegNull:
		return false
	case RegBinary:
		// Two binary blobs are equal if and only if they have the same length
		// and contain the exact same bytes. If two binary blobs have different
		// lengths, order is determined by the common bytes between the two blobs.
		// If the common bytes are equal, then the blob with the fewer bytes
		// is considered to be less than the blob with more bytes.
		// TODO: implement above
		return len(a.data.([]byte)) < len(b.data.([]byte))
	}

	return false
}

func eq(a *register, b *register) bool {
	return !less(a, b) && !less(b, a)
}
