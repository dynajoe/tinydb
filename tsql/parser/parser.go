package parser

import (
	"errors"
	"fmt"

	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/scan"
)

var topLevelStatements = []struct {
	Name  string
	Parse func(scanner scan.TinyScanner) (ast.Statement, error)
}{
	{
		Name: "CREATE",
		Parse: func(scanner scan.TinyScanner) (ast.Statement, error) {
			return parseCreateTable(scanner)
		},
	},
	{
		Name: "INSERT",
		Parse: func(scanner scan.TinyScanner) (ast.Statement, error) {
			return parseInsert(scanner)
		},
	},
	{
		Name: "SELECT",
		Parse: func(scanner scan.TinyScanner) (ast.Statement, error) {
			return parseSelect(scanner)
		},
	},
}

// ParseStatement parses a string of sql and produces a statement or parse failure.
func ParseStatement(sql string) (ast.Statement, error) {
	scanner := scan.NewScanner(sql)

	for _, p := range topLevelStatements {
		stmt, err := p.Parse(scanner)
		if err != nil {
			return nil, fmt.Errorf("[%s] parse error at character: %d\nparsed:\n\t%s",
				p.Name, scanner.Pos(), scanner.Committed())
		}
		if stmt != nil {
			return stmt, nil
		}

		scanner.Reset()
	}

	return nil, errors.New("invalid tsql program")
}
