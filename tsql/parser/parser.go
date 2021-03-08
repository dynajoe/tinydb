package parser

import (
	"errors"
	"fmt"

	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/scan"
)

var topLevelStatements = []struct {
	Name  string
	Parse func(scanner scan.TinyScanner) (ast.Statement, bool, error)
}{
	{
		Name: "CREATE",
		Parse: func(scanner scan.TinyScanner) (ast.Statement, bool, error) {
			s, err := parseCreateTable(scanner)
			return s, s != nil, err
		},
	},
	{
		Name: "INSERT",
		Parse: func(scanner scan.TinyScanner) (ast.Statement, bool, error) {
			s, err := parseInsert(scanner)
			return s, s != nil, err
		},
	},
	{
		Name: "SELECT",
		Parse: func(scanner scan.TinyScanner) (ast.Statement, bool, error) {
			s, err := parseSelect(scanner)
			return s, s != nil, err
		},
	},
	{
		Name: "BEGIN",
		Parse: func(scanner scan.TinyScanner) (ast.Statement, bool, error) {
			s, err := parseBegin(scanner)
			return s, s != nil, err
		},
	},
	{
		Name: "COMMIT",
		Parse: func(scanner scan.TinyScanner) (ast.Statement, bool, error) {
			s, err := parseCommit(scanner)
			return s, s != nil, err
		},
	},
	{
		Name: "ROLLBACK",
		Parse: func(scanner scan.TinyScanner) (ast.Statement, bool, error) {
			s, err := parseRollback(scanner)
			return s, s != nil, err
		},
	},
}

// ParseStatement parses a string of sql and produces a statement or parse failure.
func ParseStatement(sql string) (ast.Statement, error) {
	scanner := scan.NewScanner(sql)

	for _, p := range topLevelStatements {
		stmt, ok, err := p.Parse(scanner)
		if err != nil {
			return nil, fmt.Errorf("[%s] parse error at character: %d\nparsed:\n\t%s",
				p.Name, scanner.Pos(), scanner.Committed())
		}
		if ok {
			return stmt, nil
		}
		scanner.Reset()
	}

	return nil, errors.New("invalid tsql program")
}
