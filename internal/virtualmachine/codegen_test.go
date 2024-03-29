package virtualmachine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/joeandaverde/tinydb/internal/metadata"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/parser"
)

// jumpOps are the ops that contain a jump destination in P2
var jumpOps = map[Op]bool{
	OpEq: true, OpNe: true,
	OpLt: true, OpLe: true,
	OpGt: true, OpGe: true,
	OpRewind: true, OpNext: true,
}

var testTableDefs = map[string]*metadata.TableDefinition{
	"foo": {
		Name: "foo",
		Columns: []*metadata.ColumnDefinition{
			{Name: "id", Offset: 0, Type: storage.Integer},
			{Name: "email", Offset: 1, Type: storage.Text},
			{Name: "state", Offset: 2, Type: storage.Text},
		},
		RootPage: 1337,
	},
}

func TestSelectInstructions(t *testing.T) {
	r := require.New(t)

	stmt, err := parser.ParseStatement("SELECT * FROM foo")
	r.NoError(err)

	instructions := SelectInstructions(testTableDefs, stmt.(*ast.SelectStatement))
	r.NotEmpty(instructions)
	result := Instructions(instructions).String()
	r.NotEmpty(result)

	groupedByOp := groupInstructions(instructions)

	// selecting all columns
	r.Len(groupedByOp[OpColumn], len(testTableDefs["foo"].Columns))

	// next picks up at the first column load
	r.Equal(groupedByOp[OpNext][0].ixn.P2, groupedByOp[OpColumn][0].addr)

	openBtree := groupedByOp[OpOpenRead]
	r.Len(openBtree, 1)
	r.Equal(openBtree[0].ixn, &Instruction{
		Op: OpOpenRead,
		P1: 0,
		P2: testTableDefs["foo"].RootPage,
		P3: len(testTableDefs["foo"].Columns),
		P4: "foo",
	})

	r.Equal(OpHalt, instructions[len(instructions)-1].Op)

	assertJumpsValid(instructions, t)
}

func TestSelectInstructions_SingleConditionWhereClause(t *testing.T) {
	r := require.New(t)

	stmt, err := parser.ParseStatement("SELECT * FROM foo WHERE email = 'a'")
	r.NoError(err)

	instructions := SelectInstructions(testTableDefs, stmt.(*ast.SelectStatement))
	r.NotEmpty(instructions)

	assertJumpsValid(instructions, t)
}

// +----+-----------+--+--+--+--------+--+-------+
// |addr|opcode     |p1|p2|p3|p4      |p5|comment|
// +----+-----------+--+--+--+--------+--+-------+
// |0   |Init       |0 |13|0 |NULL    |0 |NULL   |
// |1   |OpenRead   |0 |3 |0 |3       |0 |NULL   |
// |2   |Rewind     |0 |12|0 |NULL    |0 |NULL   |
// |3   |Column     |0 |0 |1 |NULL    |0 |NULL   |
// |4   |Eq         |2 |7 |1 |BINARY-8|66|NULL   | (te: 7, fe: 0)
// |5   |Column     |0 |0 |1 |NULL    |0 |NULL   |
// |6   |Ne         |3 |11|1 |BINARY-8|82|NULL   | (te: 0, fe: 11)
// |7   |Column     |0 |0 |4 |NULL    |0 |NULL   |
// |8   |Column     |0 |1 |5 |NULL    |0 |NULL   |
// |9   |Column     |0 |2 |6 |NULL    |0 |NULL   |
// |10  |ResultRow  |4 |3 |0 |NULL    |0 |NULL   |
// |11  |Next       |0 |3 |0 |NULL    |1 |NULL   |
// |12  |Halt       |0 |0 |0 |NULL    |0 |NULL   |
// |13  |Transaction|0 |0 |20|0       |1 |NULL   |
// |14  |String8    |0 |2 |0 |baz     |0 |NULL   |
// |15  |String8    |0 |3 |0 |bam     |0 |NULL   |
// |16  |Goto       |0 |1 |0 |NULL    |0 |NULL   |
// +----+-----------+--+--+--+--------+--+-------+
func TestSelectInstructions2(t *testing.T) {
	r := require.New(t)

	stmt, err := parser.ParseStatement(`
		select *
		from foo
		where email = 'baz'
			OR email = 'bam'
			OR email = 'baaa'
	`)
	r.NoError(err)

	instructions := SelectInstructions(testTableDefs, stmt.(*ast.SelectStatement))
	r.NotEmpty(instructions)

	code := Instructions(instructions).String()
	r.NotEmpty(code)

	assertJumpsValid(instructions, t)
}

func TestSelectInstructions3(t *testing.T) {
	r := require.New(t)

	stmt, err := parser.ParseStatement(`
		select *
		from foo
		where email = 'baz'
			AND email != 'bam'
			OR email = 'fog'
	`)
	r.NoError(err)

	instructions := SelectInstructions(testTableDefs, stmt.(*ast.SelectStatement))
	r.NotEmpty(instructions)

	code := Instructions(instructions).String()
	r.NotEmpty(code)

	assertJumpsValid(instructions, t)
}

type groupItem struct {
	addr int
	ixn  *Instruction
}

func groupInstructions(instructions Instructions) map[Op][]groupItem {
	grouped := make(map[Op][]groupItem)
	for i, x := range instructions {
		item := groupItem{ixn: x, addr: i}
		if _, ok := grouped[x.Op]; ok {
			grouped[x.Op] = append(grouped[x.Op], item)
		} else {
			grouped[x.Op] = []groupItem{item}
		}
	}
	return grouped
}

func assertJumpsValid(instructions Instructions, t *testing.T) {
	assert := require.New(t)
	for i, x := range instructions {
		if _, ok := jumpOps[x.Op]; !ok {
			continue
		}
		jumpAddr := x.P2
		assert.NotZero(jumpAddr)
		assert.NotEqual(jumpAddr, i)
		assert.Less(jumpAddr, len(instructions))
	}
}
