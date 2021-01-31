package tsql

import (
	"fmt"

	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/lexer"
)

type Parser func(TinyScanner) (bool, interface{})

type ExpressionParser func(TinyScanner) (bool, ast.Expression)

type OpParser func(TinyScanner) (bool, string)

type nodify func(tokens []lexer.Token)

type nodifyMany func(tokens [][]lexer.Token)

type nodifyExpression func(expr ast.Expression)

type nodifyOperator func(tokens []lexer.Token) string

type expressionMaker func(op string, a ast.Expression, b ast.Expression) ast.Expression

// Parse - parses TinySql statements
func Parse(sql string) (ast.Statement, error) {
	sqlLexer := lexer.NewLexer(sql)
	scanner := &tinyScanner{
		tokens:   sqlLexer.Exec(),
		input:    sql,
		items:    []lexer.Token{},
		position: 0,
	}

	makeParseError := func(statementType string, err error) error {
		if err != nil {
			return fmt.Errorf("failed parsing [%s] at [%s]", statementType, scanner.committed)
		}

		return nil
	}

	if createStatement, err := parseCreateTable(scanner); createStatement != nil || err != nil {
		return createStatement, makeParseError("CREATE", err)
	}

	if insertStatement, err := parseInsert(scanner); insertStatement != nil || err != nil {
		return insertStatement, makeParseError("INSERT", err)
	}

	if selectStatement, err := parseSelect(scanner); selectStatement != nil || err != nil {
		return selectStatement, makeParseError("SELECT", err)
	}

	return nil, nil
}

func committed(committedAt string, p Parser) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		scanner.Commit(committedAt)
		_, reset := scanner.Mark()

		if success, results := p(scanner); success {
			return success, results
		}

		reset()
		return false, nil
	}
}

func parseTermExpression() ExpressionParser {
	return func(scanner TinyScanner) (bool, ast.Expression) {
		_, reset := scanner.Mark()
		var expr ast.Expression

		ok, _ := oneOf([]Parser{
			parseTerm(func(expression ast.Expression) {
				expr = expression
			}),
			parens(lazy(func() Parser {
				return func(scanner TinyScanner) (bool, interface{}) {
					s, e := parseExpression()(scanner)

					if s {
						expr = e
						return s, e
					}

					return false, s
				}
			})),
		}, nil)(scanner)

		if !ok {
			reset()
		}

		return ok, expr
	}
}

func makeBinaryExpression() expressionMaker {
	return func(operatorStr string, left ast.Expression, right ast.Expression) ast.Expression {
		return &ast.BinaryOperation{
			Left:     left,
			Right:    right,
			Operator: operatorStr,
		}
	}
}

func operatorParser(opParser Parser, nodifyOperator nodifyOperator) OpParser {
	return func(scanner TinyScanner) (bool, string) {
		var opText string

		success, _ := required(opParser, func(x []lexer.Token) {
			opText = nodifyOperator(x)
		})(scanner)

		return success, opText
	}
}

func comparison() OpParser {
	return operatorParser(operator(`=`), func(tokens []lexer.Token) string {
		return tokens[1].Text
	})
}

func logical() OpParser {
	return operatorParser(oneOf([]Parser{
		operator(`AND`),
		operator(`OR`),
	}, nil), func(tokens []lexer.Token) string {
		return tokens[1].Text
	})
}

func mult() OpParser {
	return operatorParser(oneOf([]Parser{
		operator(`\*`),
		operator(`/`),
	}, nil), func(tokens []lexer.Token) string {
		return tokens[1].Text
	})
}

func sum() OpParser {
	return operatorParser(oneOf([]Parser{
		operator(`\+`),
		operator(`-`),
	}, nil), func(tokens []lexer.Token) string {
		return tokens[1].Text
	})
}

func parseExpression() ExpressionParser {
	return chainl(
		chainl(
			chainl(
				chainl(
					parseTermExpression(),
					makeBinaryExpression(),
					mult(),
				),
				makeBinaryExpression(),
				sum(),
			),
			makeBinaryExpression(),
			comparison(),
		),
		makeBinaryExpression(),
		logical(),
	)
}

func parseTerm(nodify nodifyExpression) Parser {
	return oneOf([]Parser{
		requiredToken(lexer.TokenIdentifier, func(tokens []lexer.Token) {
			if nodify != nil {
				nodify(&ast.Ident{
					Value: tokens[0].Text,
				})
			}
		}),
		requiredToken(lexer.TokenString, func(tokens []lexer.Token) {
			if nodify != nil {
				nodify(&ast.BasicLiteral{
					Value: tokens[0].Text[1 : len(tokens[0].Text)-1],
					Kind:  tokens[0].Kind,
				})
			}
		}),
		requiredToken(lexer.TokenNumber, func(tokens []lexer.Token) {
			if nodify != nil {
				nodify(&ast.BasicLiteral{
					Value: tokens[0].Text,
					Kind:  tokens[0].Kind,
				})
			}
		}),
		requiredToken(lexer.TokenBoolean, func(tokens []lexer.Token) {
			if nodify != nil {
				nodify(&ast.BasicLiteral{
					Value: tokens[0].Text,
					Kind:  tokens[0].Kind,
				})
			}
		}),
		requiredToken(lexer.TokenNull, func(tokens []lexer.Token) {
			if nodify != nil {
				nodify(&ast.BasicLiteral{
					Value: "",
					Kind:  tokens[0].Kind,
				})
			}
		}),
	}, nil)
}

var optWS = optionalToken(lexer.TokenWhiteSpace)
var reqWS = requiredToken(lexer.TokenWhiteSpace, nil)
var eofParser = requiredToken(lexer.TokenEOF, nil)

func optionalToken(expected lexer.Kind) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		if scanner.Peek().Kind == expected {
			scanner.Next()
		}

		return true, nil
	}
}

func ident(n func(string)) Parser {
	return requiredToken(lexer.TokenIdentifier, func(tokens []lexer.Token) {
		n(tokens[0].Text)
	})
}

func token(expected lexer.Kind) Parser {
	return requiredToken(expected, nil)
}

func requiredToken(expected lexer.Kind, nodify nodify) Parser {
	return required(func(scanner TinyScanner) (bool, interface{}) {
		if scanner.Next().Kind == expected {
			return true, nil
		}

		return false, nil
	}, nodify)
}

func operator(operatorText string) Parser {
	return all([]Parser{
		optWS,
		regex(operatorText),
		optWS,
	}, nil)
}

func parens(inner Parser) Parser {
	return allX(
		optWS,
		requiredToken(lexer.TokenOpenParen, nil),
		optWS,
		inner,
		optWS,
		requiredToken(lexer.TokenCloseParen, nil),
		optWS,
	)
}

func parensCommaSep(p Parser) Parser {
	return parens(commaSeparated(p))
}

func commaSeparated(p Parser) Parser {
	return allX(
		optWS,
		separatedBy1(commaSeparator, p),
		optWS,
	)
}

var commaSeparator = allX(
	optWS,
	token(lexer.TokenComma),
	optWS,
)

func keyword(t lexer.Kind) Parser {
	return allX(
		optWS,
		token(t),
		oneOf([]Parser{eofParser, optWS}, nil), // Should this be required white space?
	)
}

func makeExpressionParser(nodify nodifyExpression) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		success, expr := parseExpression()(scanner)

		if success {
			nodify(expr)
		}

		return success, expr
	}
}
