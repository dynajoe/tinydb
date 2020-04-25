package engine

import (
	"errors"

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
	// Point to first entry in btree
	// P1 - Cursor
	// P2 - Jump address (if btree is empty)
	OpRewind
	// Read next cell in btree or jump to address
	// P1 - Cursor
	// P2 - Jump Address
	OpNext
	OpPrev
	OpSeek
	OpSeekGt
	OpSeekGe
	OpSeekLt
	OpSeekLe
	// P1 - cursor
	// P2 - column index (0 based)
	// P3 - register for column value
	OpColumn
	OpKey
	// Stores int in register
	// P1 - the int
	// P2 - the register
	OpInteger
	OpString
	OpNull
	// P1 - register start
	// P2 - # cols
	OpResultRow
	// P1 - register start
	// P2 - count of cols
	// P3 - store record in this register
	OpMakeRecord
	// P1 - cursor for table to get rowid
	// P2 - write rowid to this register
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
	err          string
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
		if nextPc == -1 {
			return errors.New(p.err)
		}
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
		p.writeInt(i.p2, i.p1)
	case OpString:
		r := i.p2
		s := i.p4.(string)
		reg := p.reg(r)
		reg.data = s
		reg.typ = RegString
	case OpNull:
		r := i.p2
		reg := p.reg(r)
		reg.data = nil
		reg.typ = RegNull
	case OpSCopy:
		r1 := p.reg(i.p1)
		r2 := p.reg(i.p2)
		r2.data = r1.data
		r2.typ = r1.typ
	case OpEq:
		a := p.reg(i.p1)
		jmp := i.p2
		b := p.reg(i.p3)
		if eq(a, b) {
			return jmp
		}
	case OpLt:
		a := p.reg(i.p1)
		jmp := i.p2
		b := p.reg(i.p3)
		if less(a, b) {
			return jmp
		}
	case OpLe:
		a := p.reg(i.p1)
		jmp := i.p2
		b := p.reg(i.p3)
		if less(a, b) || eq(a, b) {
			return jmp
		}
	case OpGt:
		a := p.reg(i.p1)
		jmp := i.p2
		b := p.reg(i.p3)
		if !less(a, b) && !eq(a, b) {
			return jmp
		}
	case OpGe:
		a := p.reg(i.p1)
		jmp := i.p2
		b := p.reg(i.p3)
		if !less(a, b) {
			return jmp
		}
	case OpNe:
		a := p.reg(i.p1)
		jmp := i.p2
		b := p.reg(i.p3)
		if !eq(a, b) {
			return jmp
		}
	case OpOpenRead:
		cursor := i.p1
		pageNo := i.p2
		// cols := instruction.params[2]
		f, err := storage.NewCursor(p.engine.Pager, storage.CURSOR_READ, pageNo)
		if err != nil {
			return p.error("open read error")
		}
		p.cursors[cursor] = f
	case OpOpenWrite:
		cursorIndex := i.p1
		pageNo := p.reg(i.p2).data.(int)
		// cols := instruction.params[2]
		f, err := storage.NewCursor(p.engine.Pager, storage.CURSOR_WRITE, pageNo)
		if err != nil {
			return p.error("open write error")
		}
		p.cursors[cursorIndex] = f
	case OpClose:
		cursor := p.cursors[i.p1]
		cursor.Close()
	case OpRewind:
		cursor := p.cursors[i.p1]
		jmpAddr := i.p2
		hasRecords, err := cursor.Rewind()
		if err != nil {
			return p.error("error rewinding cursor")
		}
		if !hasRecords {
			return jmpAddr
		}
	case OpNext:
		cursor := p.cursors[i.p1]
		jmpAddr := i.p2
		// no more records in cursor
		hasMore, err := cursor.Next()
		if err != nil {
			return p.error("error moving to next cell")
		}
		if !hasMore {
			return jmpAddr
		}
	case OpColumn:
		// cursor := p.cursors[i.p1]
		// col := i.p2
		// reg := p.reg(i.p3)
	case OpResultRow:
		startReg := i.p1
		colCount := i.p2
		endReg := startReg + colCount - 1
		var result []interface{}
		for i := startReg; i <= endReg; i++ {
			reg := p.reg(i)
			switch reg.typ {
			case RegInt32:
				result = append(result, reg.data.(int))
			case RegBinary:
				// TODO: should copy the buffer?
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
			return p.error("unable to allocate page for table")
		}
		if err := p.engine.Pager.Write(rootPage); err != nil {
			return p.error("unable to persist new table page")
		}
		p.writeInt(i.p1, rootPage.PageNumber)
	case OpMakeRecord:
		startReg := i.p1
		colCount := i.p2
		endReg := startReg + colCount - 1
		destReg := p.reg(i.p3)
		var fields []*storage.Field

		for i := startReg; i <= endReg; i++ {
			reg := p.reg(i)
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
				return p.error("unsupported register type for record")
			}
		}

		destReg.typ = RegRecord
		destReg.data = storage.NewRecord(fields)
	case OpRowID:
		// cursor := p.reg(i.p1)
		p.writeInt(i.p2, nextKey("master"))
	case OpInsert:
		cursor := p.cursors[i.p1]
		record := p.reg(i.p2).data.(storage.Record)
		key := p.reg(i.p3).data.(int)
		if err := cursor.Insert(key, record); err != nil {
			return p.error("error performing insert")
		}
	}

	return 0
}

func (p *program) error(message string) int {
	p.err = message
	return -1
}

func (p *program) reg(i int) *register {
	if len(p.regs) <= i {
		diff := len(p.regs) - i + 1
		// Allocate some number of registers
		for i := 0; i < diff; i++ {
			p.regs = append(p.regs, &register{
				typ:  RegUnspecified,
				data: nil,
			})
		}
	}
	return p.regs[i]
}

func (p *program) writeInt(r int, v int) {
	reg := p.reg(r)
	reg.typ = RegInt32
	reg.data = v
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