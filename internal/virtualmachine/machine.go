package virtualmachine

import (
	"errors"
	"fmt"

	"github.com/joeandaverde/tinydb/internal/storage"
)

// TODO: this is to get things to compile, need to actually get auto incr key
var keys = make(map[string]int)

// NextKey is a temporary mechanism to generate an identifier for new records
func NextKey(tableName string) int {
	if _, ok := keys[tableName]; !ok {
		keys[tableName] = 0
	}
	keys[tableName] = keys[tableName] + 1
	return keys[tableName]
}

type Op uint8

// https://github.com/uchicago-cs/chidb/blob/master/src/libchidb/dbm-types.h
const (
	OpNoOp Op = iota
	OpInit
	// Opens B-Tree Rooted at page n
	// and stores cursor in c
	// P1 - cursor (c)
	// P2 - page number (n)
	// P3 - col count (0 if opening index)
	OpOpenRead
	// Opens B-Tree Rooted at page n
	// and stores cursor in c
	// P1 - cursor (c)
	// P2 - page number register (n)
	// P3 - col count (0 if opening index)
	OpOpenWrite
	OpClose
	// Point to first entry in btree
	// P1 - Cursor
	// P2 - Jump address (if btree is empty)
	OpRewind
	// Read next cell at read cursor and go to address if more, otherwise, fallthrough.
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

type Instruction struct {
	Op Op
	P1 int
	P2 int
	P3 int
	P4 interface{}
}

type Program interface {
	Run() error
	Results() <-chan []interface{}
}

type program struct {
	pc           int
	pager        storage.Pager
	instructions []Instruction
	regs         []*register
	cursors      []*storage.Cursor
	strings      []string
	halted       bool
	results      chan []interface{}
	err          string
}

func NewProgram(pager storage.Pager, i []Instruction) Program {
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
		instructions: i,
		pc:           0,
		regs:         regs,
		pager:        pager,
		results:      make(chan []interface{}),
	}
}

func (p *program) Run() error {
	defer close(p.results)

	for p.pc < len(p.instructions) {
		fmt.Println(p.instructions[p.pc].String())
		nextPc := p.step()
		fmt.Println(nextPc, p.err)
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

func (p *program) Results() <-chan []interface{} {
	return p.results
}

func (p *program) step() int {
	i := p.instructions[p.pc]

	switch i.Op {
	case OpNoOp:
	case OpHalt:
		p.halted = true
	case OpInteger:
		p.writeInt(i.P2, i.P1)
	case OpString:
		r := i.P2
		s := i.P4.(string)
		reg := p.reg(r)
		reg.data = s
		reg.typ = RegString
	case OpNull:
		r := i.P2
		reg := p.reg(r)
		reg.data = nil
		reg.typ = RegNull
	case OpSCopy:
		r1 := p.reg(i.P1)
		r2 := p.reg(i.P2)
		r2.data = r1.data
		r2.typ = r1.typ
	case OpEq:
		a := p.reg(i.P1)
		jmp := i.P2
		b := p.reg(i.P3)
		if eq(a, b) {
			return jmp
		}
	case OpLt:
		a := p.reg(i.P1)
		jmp := i.P2
		b := p.reg(i.P3)
		if less(a, b) {
			return jmp
		}
	case OpLe:
		a := p.reg(i.P1)
		jmp := i.P2
		b := p.reg(i.P3)
		if less(a, b) || eq(a, b) {
			return jmp
		}
	case OpGt:
		a := p.reg(i.P1)
		jmp := i.P2
		b := p.reg(i.P3)
		if !less(a, b) && !eq(a, b) {
			return jmp
		}
	case OpGe:
		a := p.reg(i.P1)
		jmp := i.P2
		b := p.reg(i.P3)
		if !less(a, b) {
			return jmp
		}
	case OpNe:
		a := p.reg(i.P1)
		jmp := i.P2
		b := p.reg(i.P3)
		if !eq(a, b) {
			return jmp
		}
	case OpOpenRead:
		cursor := i.P1
		pageNo := p.reg(i.P2).data.(int)
		// cols := instruction.Params[2]
		f, err := storage.NewCursor(p.pager, storage.CURSOR_READ, pageNo)
		if err != nil {
			return p.error("open read error")
		}
		p.cursors[cursor] = f
	case OpOpenWrite:
		cursorIndex := i.P1
		pageNo := p.reg(i.P2).data.(int)
		// cols := instruction.Params[2]
		f, err := storage.NewCursor(p.pager, storage.CURSOR_WRITE, pageNo)
		if err != nil {
			return p.error("open write error")
		}
		p.cursors[cursorIndex] = f
	case OpClose:
		cursor := p.cursors[i.P1]
		cursor.Close()
	case OpRewind:
		cursor := p.cursors[i.P1]
		jmpAddr := i.P2
		hasRecords, err := cursor.Rewind()
		if err != nil {
			return p.error("error rewinding cursor")
		}
		if !hasRecords {
			return jmpAddr
		}
	case OpNext:
		cursor := p.cursors[i.P1]
		jmpAddr := i.P2
		// no more records in cursor
		hasMore, err := cursor.Next()
		if err != nil {
			return p.error("error moving to next cell")
		}
		if !hasMore {
			return jmpAddr
		}
	case OpColumn:
		cursor := p.cursors[i.P1]
		col := i.P2
		reg := p.reg(i.P3)
		record, err := cursor.CurrentCell()
		if err != nil {
			return p.error(err.Error())
		}

		field := record.Fields[col]
		reg.data = field.Data
		if field.Data == nil {
			reg.typ = RegNull
		} else {
			switch field.Type {
			case storage.Text:
				reg.typ = RegString
			case storage.Integer:
				reg.typ = RegInt32
			case storage.Byte:
				reg.typ = RegBinary
			default:
				return p.error(fmt.Sprintf("unexpected field type %v", field.Type))
			}
		}
	case OpResultRow:
		startReg := i.P1
		colCount := i.P2
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
		rootPage, err := p.pager.Allocate()
		if err != nil {
			return p.error("unable to allocate page for table")
		}
		if err := p.pager.Write(rootPage); err != nil {
			return p.error("unable to persist new table page")
		}
		p.writeInt(i.P1, rootPage.PageNumber)
	case OpMakeRecord:
		startReg := i.P1
		colCount := i.P2
		endReg := startReg + colCount - 1
		destReg := p.reg(i.P3)
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
		// cursor := p.reg(i.P1)
		p.writeInt(i.P2, NextKey("master"))
	case OpInsert:
		cursor := p.cursors[i.P1]
		record := p.reg(i.P2).data.(storage.Record)
		key := p.reg(i.P3).data.(int)
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

func (i Instruction) String() string {
	return fmt.Sprintf("%v\tp1=%d\tp2=%d\tp3=%d\tp4=%v", i.Op, i.P1, i.P2, i.P3, i.P4)
}

func (o Op) String() string {
	switch o {
	case OpNoOp:
		return "OpNoOp"
	case OpInit:
		return "OpInit"
	case OpOpenRead:
		return "OpOpenRead(cursor, page, cols)"
	case OpOpenWrite:
		return "OpOpenWrite(cursor, page, cols)"
	case OpClose:
		return "OpClose"
	case OpRewind:
		return "OpRewind(cursor, jmp)"
	case OpNext:
		return "OpNext(cursor, jmp)"
	case OpPrev:
		return "OpPrev"
	case OpSeek:
		return "OpSeek"
	case OpSeekGt:
		return "OpSeekGt"
	case OpSeekGe:
		return "OpSeekGe"
	case OpSeekLt:
		return "OpSeekLt"
	case OpSeekLe:
		return "OpSeekLe"
	case OpColumn:
		return "OpColumn(cursor, col, reg)"
	case OpKey:
		return "OpKey"
	case OpInteger:
		return "OpInteger(int, reg)"
	case OpString:
		return "OpString"
	case OpNull:
		return "OpNull"
	case OpResultRow:
		return "OpResultRow(reg, cols)"
	case OpMakeRecord:
		return "OpMakeRecord(startreg, cols, reg)"
	case OpRowID:
		return "OpRowID(cursor, reg)"
	case OpInsert:
		return "OpInsert(cursor, reg, regkey)"
	case OpEq:
		return "OpEq"
	case OpNe:
		return "OpNe"
	case OpLt:
		return "OpLt"
	case OpLe:
		return "OpLe"
	case OpGt:
		return "OpGt"
	case OpGe:
		return "OpGe"
	case OpIdxGt:
		return "OpIdxGt"
	case OpIdxGe:
		return "OpIdxGe"
	case OpIdxLt:
		return "OpIdxLt"
	case OpIdxLe:
		return "OpIdxLe"
	case OpIdxPKey:
		return "OpIdxPKey"
	case OpIdxInsert:
		return "OpIdxInsert"
	case OpCreateTable:
		return "OpCreateTable(reg)"
	case OpCreateIndex:
		return "OpCreateIndex"
	case OpCopy:
		return "OpCopy"
	case OpSCopy:
		return "OpSCopy"
	case OpHalt:
		return "OpHalt"
	}

	return string(o)
}
