package parser

import (
	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/lexer"
	"github.com/joeandaverde/tinydb/tsql/scan"
)

func parseCreateTable(scanner scan.TinyScanner) (*ast.CreateTableStatement, error) {
	createTableStatement := ast.CreateTableStatement{}
	flags := make(map[string]string)

	columnDefinition := all([]parserFn{
		optWS,
		requiredToken(lexer.TokenIdentifier, nil),
		reqWS,
		requiredToken(lexer.TokenIdentifier, nil),
		optional(all([]parserFn{
			reqWS,
			text("PRIMARY"),
			reqWS,
			text("KEY"),
		}, nil), func(tokens []lexer.Token) {
			flags["primary_key"] = "true"
		}),
		optWS,
	}, func(tokens [][]lexer.Token) {
		columnName := tokens[1][0].Text
		columnType := tokens[3][0].Text

		_, isPrimaryKey := flags["primary_key"]

		createTableStatement.Columns = append(createTableStatement.Columns, ast.ColumnDefinition{
			Name:       columnName,
			Type:       columnType,
			PrimaryKey: isPrimaryKey,
		})

		flags = make(map[string]string)
	})

	ok, _ := allX(
		keyword(lexer.TokenCreate),
		keyword(lexer.TokenTable),
		optional(
			allX(keyword(lexer.TokenIf), keyword(lexer.TokenNot), keyword(lexer.TokenExists)),
			func(tokens []lexer.Token) {
				createTableStatement.IfNotExists = true
			}),
		ident(func(tableName string) {
			createTableStatement.TableName = tableName
		}),
		parensCommaSep(columnDefinition),
	)(scanner)

	if ok {
		createTableStatement.RawText = scanner.Text()
		return &createTableStatement, nil
	}

	return nil, nil
}
