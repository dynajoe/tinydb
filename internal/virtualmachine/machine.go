package virtualmachine

import (
	"errors"
	"fmt"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/storage"
)

// TODO: this is to get things to compile, need to actually get auto incr key
var keys = make(map[string]uint32)

// NextKey is a temporary mechanism to generate an identifier for new records
func NextKey(tableName string) uint32 {
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

	// Set the database auto-commit flag to P1 (1 or 0).
	// If P2 is true, roll back any currently active btree transactions.
	// This instruction causes the VM to halt.
	OpAutoCommit

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
	// Take the logical AND of the values in registers P1 and P2 and write the result into register P3.
	// If either P1 or P2 is 0 (false) then the result is 0 even if the other input is NULL. A NULL and true or two NULLs give a NULL output.
	OpAnd
	// Add the value in register P1 to the value in register P2 and store the result in register P3. If either input is NULL, the result is NULL.
	OpAdd
	// Compare the values in register P1 and P3.
	// If reg(P3)==reg(P1) then jump to address P2.
	OpEq
	// Compare the values in register P1 and P3.
	// If reg(P3)!=reg(P1) then jump to address P2.
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

	Comment string
}

type Flags struct {
	AutoCommit bool
	Rollback   bool
}

type Program struct {
	flags        *Flags
	pc           int
	pager        pager.Pager
	instructions []*Instruction
	ps           *PreparedStatement
	regs         []*register
	cursors      []*pager.Cursor
	strings      []string
	halted       bool
	results      chan []interface{}
	err          string
}

func NewProgram(flags *Flags, p pager.Pager, ps *PreparedStatement) *Program {
	// TODO: Make this resizable
	regs := make([]*register, 10)
	for i := range regs {
		regs[i] = &register{
			typ:  RegUnspecified,
			data: nil,
		}
	}

	return &Program{
		flags:        flags,
		ps:           ps,
		cursors:      make([]*pager.Cursor, 5),
		instructions: ps.Instructions,
		pc:           0,
		regs:         regs,
		pager:        p,
		results:      make(chan []interface{}),
	}
}

func (p *Program) Run() error {
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

func (p *Program) Results() <-chan []interface{} {
	return p.results
}

func (p *Program) step() int {
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
		f, err := pager.NewCursor(p.pager, pager.CURSOR_READ, pageNo, i.P4.(string))
		if err != nil {
			return p.error("open read error")
		}
		p.cursors[cursor] = f
	case OpOpenWrite:
		cursorIndex := i.P1
		pageNo := p.reg(i.P2).data.(int)
		// cols := instruction.Params[2]
		f, err := pager.NewCursor(p.pager, pager.CURSOR_WRITE, pageNo, i.P4.(string))
		if err != nil {
			return p.error("open write error")
		}
		p.cursors[cursorIndex] = f
	case OpClose:
		p.cursors[i.P1] = nil
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
		if hasMore {
			return jmpAddr
		}
	case OpAutoCommit:
		p.flags.AutoCommit = i.P1 == 1
		p.flags.Rollback = i.P2 == 1
		p.halted = true
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
		rootPage, err := p.pager.Allocate(pager.PageTypeLeaf)
		if err != nil {
			return p.error("unable to allocate page for table")
		}
		if err := p.pager.Write(rootPage); err != nil {
			return p.error("unable to persist new table page")
		}
		p.writeInt(i.P1, rootPage.Number())
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
		destReg.data = fields
	case OpRowID:
		cursor := p.cursors[i.P1]
		p.writeInt(i.P2, int(NextKey(cursor.Name)))
	case OpInsert:
		cursor := p.cursors[i.P1]
		fields := p.reg(i.P2).data.([]*storage.Field)
		key := p.reg(i.P3).data.(int)
		record := storage.NewRecord(uint32(key), fields)
		if err := cursor.Insert(record); err != nil {
			return p.error("error performing insert")
		}
	}

	return 0
}

func (p *Program) error(message string) int {
	p.err = message
	return -1
}

func (p *Program) reg(i int) *register {
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

func (p *Program) writeInt(r int, v int) {
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
	return fmt.Sprintf("%-30v | %-4d | %-4d | %-4d | %-4v | %s", i.Op, i.P1, i.P2, i.P3, i.P4, i.Comment)
}

func (o Op) String() string {
	switch o {
	case OpNoOp:
		return "OpNoOp"
	case OpInit:
		return "OpInit"
	case OpOpenRead:
		return "OpOpenRead(cursor, page, cols, tableName)"
	case OpOpenWrite:
		return "OpOpenWrite(cursor, page, cols, tableName)"
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
