package virtualmachine

import (
	"errors"
	"fmt"
	"strings"

	"github.com/joeandaverde/tinydb/internal/metadata"
	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/lexer"
)

type program struct {
	instructions []*Instruction
	regPool      map[int]struct{}
	labelRefs    map[int]int
	readCursors  []int
}

type Instructions []*Instruction

func (i Instructions) String() string {
	var sb strings.Builder
	for addr, x := range i {
		sb.WriteString(fmt.Sprintf("| %-4d | %s |\n", addr, x.String()))
	}
	return sb.String()
}

func initProgram() *program {
	return &program{
		regPool:   make(map[int]struct{}),
		labelRefs: make(map[int]int),
	}
}

// Op0 adds an instruction that takes no params
func (p *program) Op0(op Op) int {
	p.instructions = append(p.instructions, &Instruction{Op: op, P1: 0, P2: 0, P3: 0, P4: nil})
	return len(p.instructions) - 1
}

// Op1 adds an instruction that takes 1 param
func (p *program) Op1(op Op, p1 int) int {
	p.instructions = append(p.instructions, &Instruction{Op: op, P1: p1, P2: 0, P3: 0, P4: nil})
	return len(p.instructions) - 1
}

// Op2 adds an instruction that takes 2 params
func (p *program) Op2(op Op, p1, p2 int) int {
	p.instructions = append(p.instructions, &Instruction{Op: op, P1: p1, P2: p2, P3: 0, P4: nil})
	return len(p.instructions) - 1
}

// Op3 adds an instruction that takes 3 params
func (p *program) Op3(op Op, p1, p2, p3 int) int {
	p.instructions = append(p.instructions, &Instruction{Op: op, P1: p1, P2: p2, P3: p3, P4: nil})
	return len(p.instructions) - 1
}

// Op4 adds an instruction that takes 4 params
func (p *program) Op4(op Op, p1, p2, p3 int, p4 interface{}) int {
	p.instructions = append(p.instructions, &Instruction{Op: op, P1: p1, P2: p2, P3: p3, P4: p4})
	return len(p.instructions) - 1
}
func (p *program) Comment(s string) {
	p.instructions[len(p.instructions)-1].Comment = s
}

func (p *program) OpString(reg int, s string) int {
	return p.Op4(OpString, len(s), int(reg), x, s)
}

func (p *program) OpInt(reg int, value int) int {
	return p.Op2(OpInteger, value, int(reg))
}

func (p *program) OpNull(reg int) int {
	return p.Op2(OpNull, x, reg)
}

func (p *program) OpHalt() int {
	return p.Op0(OpHalt)
}

func (p *program) MakeLabel() int {
	labelRef := -len(p.labelRefs) - 1
	p.labelRefs[labelRef] = labelRef
	return labelRef
}

func (p *program) EmitLabel(labelRef int) {
	p.labelRefs[labelRef] = len(p.instructions)
}

func (p *program) ReadCursor(page int) int {
	p.readCursors = append(p.readCursors, page)
	return len(p.readCursors) - 1
}

func (p *program) RegAlloc() int {
	for i := 0; i < 100; i++ {
		if _, ok := p.regPool[i]; !ok {
			p.regPool[i] = struct{}{}
			return i
		}
	}

	panic("who so many registers batman?")
}

func (p *program) RegAllocN(num int) int {
	remaining := num
	startReg := 0
	for ; startReg < 100; startReg++ {
		_, ok := p.regPool[startReg]
		// if the reg is taken, reset our count.
		if ok {
			remaining = num
		} else {
			remaining--
		}

		// If we got all contiguous regs, done.
		if remaining == 0 {
			break
		}
	}

	return startReg
}

func (p *program) RegRelease(r int) {
	if _, ok := p.regPool[r]; ok {
		delete(p.regPool, r)
		return
	}
	panic("attempt to release a register that wasnt allocated")
}

const x = 0

// CreateTableInstructions creates instructions from create table statement
//
// Example from SQLite
// +------+-------------+----+----+----+--------------------------------------+----+---------+
// | addr |   opcode    | p1 | p2 | p3 |                  p4                  | p5 | comment |
// +------+-------------+----+----+----+--------------------------------------+----+---------+
// |    0 | Init        |  0 | 38 |  0 |                                      | 00 |         |
// |    1 | ReadCookie  |  0 |  3 |  2 |                                      | 00 |         |
// |    2 | If          |  3 |  5 |  0 |                                      | 00 |         |
// |    3 | SetCookie   |  0 |  2 |  4 |                                      | 00 |         |
// |    4 | SetCookie   |  0 |  5 |  1 |                                      | 00 |         |
// |    5 | CreateBtree |  0 |  2 |  1 |                                      | 00 |         |
// |    6 | OpenWrite   |  0 |  1 |  0 | 5                                    | 00 |         |
// |    7 | NewRowid    |  0 |  1 |  0 |                                      | 00 |         |
// |    8 | Blob        |  6 |  3 |  0 |                                     | 00 |         |
// |    9 | Insert      |  0 |  3 |  1 |                                      | 08 |         |
// |   10 | Close       |  0 |  0 |  0 |                                      | 00 |         |
// |   11 | Noop        |  0 | 22 |  0 |                                      | 00 |         |
// |   12 | CreateBtree |  0 |  4 |  2 |                                      | 00 |         |
// |   13 | OpenWrite   |  1 |  1 |  0 | 5                                    | 00 |         |
// |   14 | NewRowid    |  1 |  5 |  0 |                                      | 00 |         |
// |   15 | String8     |  0 |  6 |  0 | index                                | 00 |         |
// |   16 | String8     |  0 |  7 |  0 | sqlite_autoindex_foo_1               | 00 |         |
// |   17 | String8     |  0 |  8 |  0 | foo                                  | 00 |         |
// |   18 | Copy        |  4 |  9 |  0 |                                      | 00 |         |
// |   19 | Null        |  0 | 10 |  0 |                                      | 00 |         |
// |   20 | MakeRecord  |  6 |  5 | 11 | BBBDB                                | 00 |         |
// |   21 | Insert      |  1 | 11 |  5 |                                      | 18 |         |
// |   22 | Close       |  0 |  0 |  0 |                                      | 00 |         |
// |   23 | Null        |  0 | 12 | 13 |                                      | 00 |         |
// |   24 | OpenWrite   |  2 |  1 |  0 | 5                                    | 00 |         |
// |   25 | SeekRowid   |  2 | 27 |  1 |                                      | 00 |         |
// |   26 | Rowid       |  2 | 13 |  0 |                                      | 00 |         |
// |   27 | IsNull      | 13 | 35 |  0 |                                      | 00 |         |
// |   28 | String8     |  0 | 14 |  0 | table                                | 00 |         |
// |   29 | String8     |  0 | 15 |  0 | foo                                  | 00 |         |
// |   30 | String8     |  0 | 16 |  0 | foo                                  | 00 |         |
// |   31 | Copy        |  2 | 17 |  0 |                                      | 00 |         |
// |   32 | String8     |  0 | 18 |  0 | CREATE TABLE foo (x int primary key) | 00 |         |
// |   33 | MakeRecord  | 14 |  5 | 19 | BBBDB                                | 00 |         |
// |   34 | Insert      |  2 | 19 | 13 |                                      | 00 |         |
// |   35 | SetCookie   |  0 |  1 |  4 |                                      | 00 |         |
// |   36 | ParseSchema |  0 |  0 |  0 | tbl_name='foo' AND type!='trigger'   | 00 |         |
// |   37 | Halt        |  0 |  0 |  0 |                                      | 00 |         |
// |   38 | Transaction |  0 |  1 |  3 | 0                                    | 01 |         |
// |   39 | Goto        |  0 |  1 |  0 |                                      | 00 |         |
// +------+-------------+----+----+----+--------------------------------------+----+---------+
// Generated by https://ozh.github.io/ascii-tables/
func CreateTableInstructions(stmt *ast.CreateTableStatement) []*Instruction {
	p := initProgram()

	// The system table
	rootPage := 1

	// Open database file cursor and store cursor at [Cur 0] with 5 columns
	openCursor := 0
	p.Op4(OpOpenWrite, openCursor, rootPage, 5, ".schema")

	// Master table entry [Reg 1-5]
	masterTable1Reg := p.RegAlloc()
	masterTable2Reg := p.RegAlloc()
	masterTable3Reg := p.RegAlloc()
	masterTable4Reg := p.RegAlloc()
	masterTable5Reg := p.RegAlloc()

	// Data is in order of the master table columns
	// Create new table and store root page in [Reg 4]
	p.Op1(OpCreateTable, masterTable4Reg)

	// Store strings in registers
	p.OpString(masterTable1Reg, "table")
	p.OpString(masterTable2Reg, stmt.TableName)
	p.OpString(masterTable3Reg, stmt.TableName)
	p.OpString(masterTable5Reg, stmt.RawText)

	// Make record from [Reg 1-5], store in [Reg 6]
	recordReg := p.RegAlloc()
	p.Op3(OpMakeRecord, masterTable1Reg, 5, recordReg)

	// Acquire a rowid for the new record, store in [Reg 7]
	rowIDReg := p.RegAlloc()
	p.Op2(OpRowID, openCursor, rowIDReg)

	// Insert record to [Cur 0], record from [Reg 6], key from [Reg 7]
	p.Op3(OpInsert, openCursor, recordReg, rowIDReg)
	p.Op1(OpClose, openCursor)
	p.OpHalt()

	return p.instructions
}

// InsertInstructions generates machine code for insert statement
//
// SQLite Example
//
// Based on: CREATE TABLE company (company_id int PRIMARY KEY, company_name text, description text)
//
// EXPLAIN INSERT INTO company (company_id, company_name, description) VALUES (99, 'hashicorp', NULL)
// +------+-------------+------+----+----+--------------------+----+---------+
// | addr |   opcode    |  p1  | p2 | p3 |         p4         | p5 | comment |
// +------+-------------+------+----+----+--------------------+----+---------+
// |    0 | Init        |    0 | 17 |  0 |                    | 00 |         |
// |    1 | OpenWrite   |    0 |  2 |  0 | 3                  | 00 |         |
// |    2 | OpenWrite   |    1 |  3 |  0 | k(2,,)             | 00 |         |
// |    3 | NewRowid    |    0 |  1 |  0 |                    | 00 |         |
// |    4 | Integer     |   99 |  2 |  0 |                    | 00 |         |
// |    5 | String8     |    0 |  3 |  0 | hashicorp          | 00 |         |
// |    6 | Null        |    0 |  4 |  0 |                    | 00 |         |
// |    7 | Affinity    |    2 |  3 |  0 | DBB                | 00 |         |
// |    8 | SCopy       |    2 |  6 |  0 |                    | 00 |         |
// |    9 | IntCopy     |    1 |  7 |  0 |                    | 00 |         |
// |   10 | MakeRecord  |    6 |  2 |  5 |                    | 00 |         |
// |   11 | NoConflict  |    1 | 13 |  6 | 1                  | 00 |         |
// |   12 | Halt        | 1555 |  2 |  0 | company.company_id | 02 |         |
// |   13 | IdxInsert   |    1 |  5 |  6 | 2                  | 10 |         |
// |   14 | MakeRecord  |    2 |  3 |  8 |                    | 00 |         |
// |   15 | Insert      |    0 |  8 |  1 | company            | 39 |         |
// |   16 | Halt        |    0 |  0 |  0 |                    | 00 |         |
// |   17 | Transaction |    0 |  1 |  5 | 0                  | 01 |         |
// |   18 | Goto        |    0 |  1 |  0 |                    | 00 |         |
// +------+-------------+------+----+----+--------------------+----+---------+
//
// Without Primary Key
// +------+-------------+----+----+----+-----------+----+---------+
// | addr |   opcode    | p1 | p2 | p3 |    p4     | p5 | comment |
// +------+-------------+----+----+----+-----------+----+---------+
// |    0 | Init        |  0 |  9 |  0 |           | 00 |         |
// |    1 | OpenWrite   |  0 |  2 |  0 | 3         | 00 |         |
// |    2 | NewRowid    |  0 |  1 |  0 |           | 00 |         |
// |    3 | Integer     | 99 |  2 |  0 |           | 00 |         |
// |    4 | String8     |  0 |  3 |  0 | hashicorp | 00 |         |
// |    5 | Null        |  0 |  4 |  0 |           | 00 |         |
// |    6 | MakeRecord  |  2 |  3 |  5 | DBB       | 00 |         |
// |    7 | Insert      |  0 |  5 |  1 | company   | 39 |         |
// |    8 | Halt        |  0 |  0 |  0 |           | 00 |         |
// |    9 | Transaction |  0 |  1 |  7 | 0         | 01 |         |
// |   10 | Goto        |  0 |  1 |  0 |           | 00 |         |
// +------+-------------+----+----+----+-----------+----+---------+
func InsertInstructions(pager pager.Pager, stmt *ast.InsertStatement) []*Instruction {
	table, err := metadata.GetTableDefinition(pager, stmt.Table)
	if err != nil {
		return nil
	}

	p := initProgram()

	// Register to store the rowid
	rowIDReg := p.RegAlloc()

	// Allocate registers for each column value
	// TODO: This should find a contiguous block of regs
	firstReg := p.RegAlloc()
	for i := 1; i < len(table.Columns); i++ {
		p.RegAlloc()
	}

	// If there's a returning statement build an easy lookup
	var returnRegs []int
	returningLookup := make(map[string]int, len(stmt.Returning))
	for i, c := range stmt.Returning {
		returningLookup[c] = i
	}

	// Table cursor
	cursorIndex := 0

	// Open the root page for writing
	p.Op4(OpOpenWrite, cursorIndex, table.RootPage, len(table.Columns), table.Name)

	// RowID for table
	p.Op2(OpRowID, cursorIndex, rowIDReg)

	// Populate registers with values to be inserted
	for i, column := range table.Columns {
		reg := firstReg + i

		if _, ok := returningLookup[column.Name]; ok {
			returnRegs = append(returnRegs, reg)
		}

		// If there's no value that maps to the table column
		// use the default from table defition.
		expr, ok := stmt.Values[column.Name]
		if !ok {
			p.AddValue(reg, column, column.DefaultValue)
			continue
		}

		// TODO: generate instructions rather than evaluating the expression during codegen (incorrect).
		v := Evaluate(expr, nil)
		p.AddValue(reg, column, v.Value)
	}

	// Make the record and store in a register
	recordReg := p.RegAlloc()
	p.Op3(OpMakeRecord, firstReg, len(table.Columns), recordReg)

	// Insert the record to the btree, store rowid in reg
	p.Op3(OpInsert, cursorIndex, recordReg, rowIDReg)

	// // Returning statement
	// if len(returnRegs) > 0 {
	// 	regReturnStart := regNewRecord + 1
	// 	for i, r := range returnRegs {
	// 		// Copy the original reg value to the new reg in order to be contiguous
	// 		instructions = append(instructions, Instruction{OpSCopy, r, regReturnStart + i, x, x})
	// 	}
	// 	instructions = append(instructions, []*Instruction{
	// 		{OpResultRow, regReturnStart, len(returnRegs), x, x},
	// 	}...)
	// 	regsUsed = regsUsed + len(returnRegs)
	// }

	// All done
	p.OpHalt()

	return p.instructions
}

func (p *program) AddValue(reg int, column *metadata.ColumnDefinition, value interface{}) int {
	// Supplied value and column type must match up
	switch v := value.(type) {
	case string:
		if column.Type != storage.Text {
			panic("type conversion not implemented")
		}
		return p.OpString(reg, v)
	case int:
		if column.Type != storage.Integer {
			panic("type conversion not implemented")
		}
		return p.OpInt(reg, v)
	case byte:
		if column.Type != storage.Byte {
			panic("type conversion not implemented")
		}
		return p.OpInt(reg, int(v))
	case nil:
		return p.OpNull(reg)
	default:
		panic("unsupported type")
	}
}

func (p *program) Finalize() {
	for _, instruction := range p.instructions {
		// If P2 is a negative number it is a reference to a labeled instruction
		if instruction.P2 < 0 {
			instruction.P2 = p.labelRefs[instruction.P2]
		}
	}
}

// SelectInstructions generates instructions from a select statement to generate rows
//
// Query: SELECT * from company WHERE company_name = 'joe'
// +------+-------------+----+----+----+----------+----+---------+
// | addr |   opcode    | p1 | p2 | p3 |    p4    | p5 | comment |
// +------+-------------+----+----+----+----------+----+---------+
// |    0 | Init        |  0 | 11 |  0 |          | 00 |         |
// |    1 | OpenRead    |  0 |  2 |  0 | 3        | 00 |         |
// |    2 | Rewind      |  0 | 10 |  0 |          | 00 |         |
// |    3 | Column      |  0 |  1 |  1 |          | 00 |         |
// |    4 | Ne          |  2 |  9 |  1 | (BINARY) | 52 |         |
// |    5 | Column      |  0 |  0 |  3 |          | 00 |         |
// |    6 | Column      |  0 |  1 |  4 |          | 00 |         |
// |    7 | Column      |  0 |  2 |  5 |          | 00 |         |
// |    8 | ResultRow   |  3 |  3 |  0 |          | 00 |         |
// |    9 | Next        |  0 |  3 |  0 |          | 01 |         |
// |   10 | Halt        |  0 |  0 |  0 |          | 00 |         |
// |   11 | Transaction |  0 |  0 |  7 | 0        | 01 |         |
// |   12 | String8     |  0 |  2 |  0 | joe      | 00 |         |
// |   13 | Goto        |  0 |  1 |  0 |          | 00 |         |
// +------+-------------+----+----+----+----------+----+---------+
func SelectInstructions(tableDefs map[string]*metadata.TableDefinition, stmt *ast.SelectStatement) []*Instruction {
	table, ok := tableDefs[stmt.From[0].Name]
	if !ok {
		return []*Instruction{}
	}

	// TODO: this will also need to handle aliased tables
	colLookup := make(map[string]*metadata.ColumnDefinition, len(table.Columns))
	for _, c := range table.Columns {
		colLookup[c.Name] = c
	}

	// Build references to the columns being returned
	selectCols := make([]*metadata.ColumnDefinition, 0, len(stmt.Columns))
	for _, c := range stmt.Columns {
		if c == "*" {
			selectCols = append(selectCols, table.Columns...)
			continue
		}
		selectCols = append(selectCols, colLookup[c])
	}

	p := initProgram()

	// Set up a read cursor for the root page of the table
	readCursor := p.ReadCursor(table.RootPage)

	// Allocate registers for result columns
	firstColReg := p.RegAllocN(len(selectCols))

	// Set up labels for control flow
	haltLabel := p.MakeLabel()
	nextLabel := p.MakeLabel()
	recordLabel := p.MakeLabel()
	evalLabel := p.MakeLabel()

	// Open table for reading
	p.Op4(OpOpenRead, readCursor, table.RootPage, len(selectCols), table.Name)

	// Go to first entry in btree or go to halt
	p.Op2(OpRewind, readCursor, haltLabel)

	// Add instructions to check against each row
	p.EmitLabel(evalLabel)
	if stmt.Filter != nil {
		transformedExpr := reworkExpression(stmt.Filter)
		where := whereClause{p: p, tableDefs: tableDefs}
		where.emit(transformedExpr, evalContext{
			te:          recordLabel,
			fe:          nextLabel,
			conjunction: true,
		})
	}

	// Load selected columns into registers
	for i, c := range selectCols {
		p.Op3(OpColumn, readCursor, c.Offset, firstColReg+i)
	}

	// Produce a Row
	p.EmitLabel(recordLabel)
	p.Op2(OpResultRow, firstColReg, len(selectCols))

	// Move cursor to next record and go to address if success, otherwise, fallthrough
	p.EmitLabel(nextLabel)
	p.Op2(OpNext, readCursor, evalLabel)

	// Set the jump address for halt if there are no records
	p.EmitLabel(haltLabel)
	p.OpHalt()

	// Load all literals into registers

	// Finalize the program to return complete instructions
	p.Finalize()

	return p.instructions
}

func BeginInstructions(stmt *ast.BeginStatement) []*Instruction {
	p := initProgram()

	p.Op1(OpAutoCommit, 0)
	p.OpHalt()

	return p.instructions
}

func CommitInstructions(stmt *ast.CommitStatement) []*Instruction {
	p := initProgram()

	p.Op1(OpAutoCommit, 1)
	p.OpHalt()

	return p.instructions
}

func RollbackInstructions(stmt *ast.RollbackStatement) []*Instruction {
	p := initProgram()

	p.Op2(OpAutoCommit, 1, 1)
	p.OpHalt()

	return p.instructions
}

type evalContext struct {
	conjunction bool
	disjunction bool
	te          int
	fe          int
}

type whereClause struct {
	p         *program
	tableDefs map[string]*metadata.TableDefinition
}

func (c whereClause) emit(expr ast.Expression, evalCtx evalContext) int {
	switch e := expr.(type) {
	case *ast.LogicalOperation:
		return c.emitLogicalExpression(e, evalCtx)
	case *ast.BinaryOperation:
		return c.emitBinaryOperation(e, evalCtx)
	case *ast.BasicLiteral:
		litReg := c.p.RegAlloc()
		switch e.Kind {
		case lexer.TokenString:
			c.p.OpString(litReg, e.Value)
		}
		return litReg
	case *ast.Ident:
		// Find the table and cursor
		_, columnDef, err := c.emitIdent(e.Value)
		if err != nil {
			panic(err)
		}
		// TODO: get correct read cursor
		colReg := c.p.RegAlloc()
		c.p.Op3(OpColumn, 0, columnDef.Offset, colReg)
		return colReg
	default:
		panic("unexpected expression type")
	}
}

func (c whereClause) emitLogicalExpression(e *ast.LogicalOperation, evalCtx evalContext) int {
	switch e.Operator {
	case "OR":
		trueLabel := c.p.MakeLabel()
		lastTermIndex := len(e.Terms) - 1
		for i, t := range e.Terms {
			// If any term evaluates to true, short circuit evaluation
			if i != lastTermIndex {
				falseExit := c.p.MakeLabel()
				c.emit(t, evalContext{te: trueLabel, fe: falseExit, disjunction: true})
				c.p.EmitLabel(falseExit)
			} else {
				c.emit(t, evalContext{fe: evalCtx.fe, conjunction: true})
			}
		}
		c.p.EmitLabel(trueLabel)
	case "AND":
		trueLabel := c.p.MakeLabel()
		lastTermIndex := len(e.Terms) - 1
		for i, t := range e.Terms {
			if i != lastTermIndex {
				c.emit(t, evalContext{fe: evalCtx.fe, conjunction: true})
			} else {
				c.emit(t, evalContext{te: evalCtx.te, conjunction: true})
			}
		}
		c.p.EmitLabel(trueLabel)
	default:
		panic("unexpected logical operator")
	}

	return -1
}

func (c whereClause) emitIdent(ident string) (*metadata.TableDefinition, *metadata.ColumnDefinition, error) {
	// TODO: Make this efficient and use table aliases
	for _, t := range c.tableDefs {
		for _, c := range t.Columns {
			if c.Name == ident {
				return t, c, nil
			}
		}
	}
	return nil, nil, errors.New("cannot resolve ident")
}

func (c whereClause) emitBinaryOperation(o *ast.BinaryOperation, evalCtx evalContext) int {
	switch o.Operator {
	case "=":
		leftReg := c.emit(o.Left, evalContext{})
		rightReg := c.emit(o.Right, evalContext{})
		if evalCtx.conjunction {
			c.p.Op3(OpNe, leftReg, evalCtx.fe, rightReg)
		} else if evalCtx.disjunction {
			c.p.Op3(OpEq, leftReg, evalCtx.te, rightReg)
		} else {
			panic("unknown logical context")
		}

		c.p.Comment(o.String())
		return -1
	case "!=":
		leftReg := c.emit(o.Left, evalContext{})
		rightReg := c.emit(o.Right, evalContext{})
		if evalCtx.te != 0 {
			c.p.Op3(OpNe, leftReg, evalCtx.te, rightReg)
		} else {
			c.p.Op3(OpEq, leftReg, evalCtx.fe, rightReg)
		}
		c.p.Comment(o.String())
		return -1
	}

	panic("unexpected operator")
}

func reworkExpression(expr ast.Expression) ast.Expression {
	logicalGrouper := logicalGrouper{}
	return logicalGrouper.Visit(expr)
}

type Visitor interface {
	Visit(ast.Expression) ast.Expression
}

type logicalGrouper struct{}

func (g logicalGrouper) Visit(expr ast.Expression) ast.Expression {
	switch e := expr.(type) {
	case *ast.BinaryOperation:
		switch e.Operator {
		case "OR", "AND":
			result := &ast.LogicalOperation{
				Operator: e.Operator,
			}

			leftExpr := g.Visit(e.Left)
			if leftTerm, ok := leftExpr.(*ast.LogicalOperation); ok && leftTerm.Operator == e.Operator {
				result.Terms = leftTerm.Terms
			} else {
				result.Terms = append(result.Terms, leftExpr)
			}

			rightExpr := g.Visit(e.Right)
			if rightTerm, ok := rightExpr.(*ast.LogicalOperation); ok && rightTerm.Operator == e.Operator {
				result.Terms = rightTerm.Terms
			} else {
				result.Terms = append(result.Terms, rightExpr)
			}

			return result
		}
	}

	return expr
}
