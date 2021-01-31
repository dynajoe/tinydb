package parser

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/scan"
)

func Test_parseSelect(t *testing.T) {
	assert := require.New(t)

	scanner := scan.NewScanner(`
		SELECT * FROM apples
	`)

	stmt, err := parseSelect(scanner)

	assert.NotNil(stmt)
	assert.NoError(err)
	assert.Equal(&ast.SelectStatement{
		From:    []ast.TableAlias{{Name: "apples", Alias: ""}},
		Columns: []string{"*"},
		Filter:  nil,
	}, stmt)
}
