package virtualmachine

import (
	"context"
	"errors"
	"fmt"
	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/storage"
)

type Flags struct {
	AutoCommit bool
	Rollback   bool
}

type Output struct {
	Data []interface{}
}

type Program struct {
	pid          int
	instructions []*Instruction
	regs         []*register
	cursors      []*pager.Cursor
	pc           int
	halted       bool
	out          chan Output
	err          string
}

func NewProgram(pid int, stmt *PreparedStatement) *Program {
	// TODO: Make this resizable
	regs := make([]*register, 10)
	for i := range regs {
		regs[i] = &register{
			typ:  RegUnspecified,
			data: nil,
		}
	}

	return &Program{
		pid:          pid,
		pc:           0,
		cursors:      make([]*pager.Cursor, 5),
		instructions: stmt.Instructions,
		regs:         regs,
		out:          make(chan Output),
	}
}

func (p *Program) Run(ctx context.Context, flags Flags, pgr pager.Pager) (Flags, error) {
	defer close(p.out)
	for p.pc < len(p.instructions) {
		nextPc := p.step(ctx, &flags, pgr)
		if nextPc == -1 {
			return Flags{
				AutoCommit: false,
				Rollback:   true,
			}, errors.New(p.err)
		}

		if p.halted {
			break
		}
		if nextPc > 0 {
			p.pc = nextPc
			continue
		}
		p.pc = p.pc + 1
	}
	return flags, nil
}

func (p *Program) Pid() int {
	return p.pid
}

func (p *Program) Output() <-chan Output {
	return p.out
}

func (p *Program) step(ctx context.Context, flags *Flags, pgr pager.Pager) int {
	i := p.instructions[p.pc]

	switch i.Op {
	case OpNoOp:
	case OpHalt:
		p.halted = true
	case OpInteger:
		p.setIntReg(i.P2, i.P1)
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
		f, err := pager.NewCursor(pgr, pager.CURSOR_READ, pageNo, i.P4.(string))
		if err != nil {
			return p.error("open read error")
		}
		p.cursors[cursor] = f
	case OpOpenWrite:
		cursorIndex := i.P1
		pageNo := p.reg(i.P2).data.(int)
		// cols := instruction.Params[2]
		f, err := pager.NewCursor(pgr, pager.CURSOR_WRITE, pageNo, i.P4.(string))
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
		flags.AutoCommit = i.P1 == 1
		flags.Rollback = i.P2 == 1
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

		select {
		case <-ctx.Done():
			p.halted = true
		case p.out <- Output{Data: result}:
		}
	case OpCreateTable:
		// Allocate a page for the new table
		rootPage, err := pgr.Allocate(pager.PageTypeLeaf)
		if err != nil {
			return p.error(fmt.Sprintf("unable to allocate page for table: %s", err.Error()))
		}
		if err := pgr.Write(rootPage); err != nil {
			return p.error(fmt.Sprintf("unable to persist new table page: %s", err.Error()))
		}
		p.setIntReg(i.P1, rootPage.Number())
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
		p.setIntReg(i.P2, int(nextKey(cursor.Name)))
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

func (p *Program) setIntReg(r int, v int) {
	reg := p.reg(r)
	reg.typ = RegInt32
	reg.data = v
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
