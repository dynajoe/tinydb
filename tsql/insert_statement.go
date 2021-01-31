package tsql

import (
	"fmt"

	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/lexer"
)

func parseInsert(scanner TinyScanner) (*ast.InsertStatement, error) {
	insertTableStatement := ast.InsertStatement{}

	var columns []string
	var values []ast.Expression

	returningClause := allX(
		keyword(lexer.TokenReturning),
		committed("RETURNING_COLUMNS", commaSeparated(
			oneOf([]Parser{
				token(lexer.TokenIdentifier),
				token(lexer.TokenAsterisk),
			}, func(tokens []lexer.Token) {
				insertTableStatement.Returning = append(insertTableStatement.Returning, tokens[0].Text)
			}),
		)),
	)

	scanner.Reset()
	ok, _ := allX(
		keyword(lexer.TokenInsert),
		keyword(lexer.TokenInto),
		ident(func(tableName string) {
			insertTableStatement.Table = tableName
		}),
		parensCommaSep(
			ident(func(column string) {
				columns = append(columns, column)
			}),
		),
		keyword(lexer.TokenValues),
		parensCommaSep(
			makeExpressionParser(func(e ast.Expression) {
				values = append(values, e)
			}),
		),
		optionalX(returningClause),
	)(scanner)

	if !ok {
		return nil, nil
	}

	// if columns and values are not of same length or are empty blow up
	// create map
	numColumns := len(columns)
	numValues := len(values)

	if numColumns != numValues {
		return nil, fmt.Errorf("unexpected number of values")
	}

	insertTableStatement.Values = make(map[string]ast.Expression)

	for i := 0; i < numColumns; i++ {
		insertTableStatement.Values[columns[i]] = values[i]
	}

	return &insertTableStatement, nil
}
