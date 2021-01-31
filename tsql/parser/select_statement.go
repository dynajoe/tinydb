package parser

import (
	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/lexer"
	"github.com/joeandaverde/tinydb/tsql/scan"
)

func parseSelect(scanner scan.TinyScanner) (*ast.SelectStatement, error) {
	selectStatement := ast.SelectStatement{}

	whereClause := allX(
		keyword(lexer.TokenWhere),
		committed("WHERE", makeExpressionParser(func(filter ast.Expression) {
			selectStatement.Filter = filter
		})),
	)

	ok, _ := allX(
		committed("SELECT", keyword(lexer.TokenSelect)),
		committed("COLUMNS", commaSeparated(
			oneOf([]Parser{
				token(lexer.TokenIdentifier),
				token(lexer.TokenAsterisk),
			}, func(tokens []lexer.Token) {
				selectStatement.Columns = append(selectStatement.Columns, tokens[0].Text)
			}),
		)),
		committed("FROM", keyword(lexer.TokenFrom)),
		committed("RELATIONS", commaSeparated(
			all([]Parser{
				committed("RELATION", token(lexer.TokenIdentifier)),
				optionalX(allX(
					reqWS,
					token(lexer.TokenIdentifier),
				)),
			}, func(tokens [][]lexer.Token) {
				if len(tokens[1]) > 0 {
					selectStatement.From = append(selectStatement.From, ast.TableAlias{
						Name:  tokens[0][0].Text,
						Alias: tokens[1][1].Text,
					})
				} else {
					selectStatement.From = append(selectStatement.From, ast.TableAlias{
						Name:  tokens[0][0].Text,
						Alias: "",
					})
				}
			}),
		)),
		optionalX(whereClause),
	)(scanner)

	if ok {
		return &selectStatement, nil
	}

	return nil, nil
}
