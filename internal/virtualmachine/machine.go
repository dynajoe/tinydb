package virtualmachine

import (
	"fmt"
)

// TODO: this is to get things to compile, need to actually get auto incr key
var keys = make(map[string]uint32)

// Register Types
type reg uint

const (
	RegUnspecified reg = iota
	RegNull
	RegInt32
	RegString
	RegBinary
	RegRecord
)

// Op Codes
type Op uint8

const (
	OpNoOp Op = iota
	OpInit
	// Opens B-Tree Rooted at page n
	// and stores cursor in c
	// 	P1 - cursor (c)
	// 	P2 - page number (n)
	// 	P3 - col count (0 if opening index)
	OpOpenRead
	// Opens B-Tree Rooted at page n
	// and stores cursor in c
	// 	P1 - cursor (c)
	// 	P2 - page number register (n)
	// 	P3 - col count (0 if opening index)
	OpOpenWrite
	OpClose
	// Point to first entry in btree
	// 	P1 - Cursor
	// 	P2 - Jump address (if btree is empty)
	OpRewind
	// Read next cell at read cursor and go to address if more, otherwise, fallthrough.
	// 	P1 - Cursor
	// 	P2 - Jump Address
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

	// 	P1 - cursor
	// 	P2 - column index (0 based)
	// 	P3 - register for column value
	OpColumn
	OpKey
	// Stores int in register
	// 	P1 - the int
	// 	P2 - the register
	OpInteger
	OpString
	OpNull
	// 	P1 - register start
	// 	P2 - # cols
	OpResultRow
	// 	P1 - register start
	// 	P2 - count of cols
	// 	P3 - store record in this register
	OpMakeRecord
	// 	P1 - cursor for table to get rowid
	// 	P2 - write rowid to this register
	OpRowID
	// 	P1 - cursor
	// 	P2 - register containing the record
	// 	P3 - register with record key
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
	// 	P1 - register for root page
	OpCreateTable
	OpCreateIndex
	OpCopy
	OpSCopy
	OpHalt
)

type Instruction struct {
	Op Op
	P1 int
	P2 int
	P3 int
	P4 interface{}

	Comment string
}

type register struct {
	typ  reg
	data interface{}
}

// nextKey is a temporary mechanism to generate an identifier for new records
func nextKey(tableName string) uint32 {
	if _, ok := keys[tableName]; !ok {
		keys[tableName] = 0
	}
	keys[tableName] = keys[tableName] + 1
	return keys[tableName]
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
		return "OpOpenRead(cur, pg, cols, tbl)"
	case OpOpenWrite:
		return "OpOpenWrite(cur, pg, cols, tbl)"
	case OpClose:
		return "OpClose"
	case OpRewind:
		return "OpRewind(cur, jmp)"
	case OpNext:
		return "OpNext(cur, jmp)"
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
		return "OpColumn(cur, col, reg)"
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
		return "OpRowID(cur, reg)"
	case OpInsert:
		return "OpInsert(cur, reg, regkey)"
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
