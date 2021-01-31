package tsql

import (
	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/parser"
)

// Parse parses TinySQL language and produces an AST.
func Parse(sql string) (ast.Statement, error) {
	return parser.ParseStatement(sql)
}
