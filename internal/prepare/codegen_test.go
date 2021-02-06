package prepare

import (
	"testing"

	"github.com/joeandaverde/tinydb/internal/metadata"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/parser"
	"github.com/stretchr/testify/require"
)

var testTableDefs = map[string]*metadata.TableDefinition{
	"foo": {
		Name: "foo",
		Columns: []metadata.ColumnDefinition{
			{Name: "id", Offset: 0, Type: storage.Integer},
			{Name: "email", Offset: 1, Type: storage.Text},
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
	r.Equal("", result)
}
