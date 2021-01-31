package parser

import (
	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/lexer"
	"github.com/joeandaverde/tinydb/tsql/scan"
)

type expressionParserFn func(scan.TinyScanner) (bool, ast.Expression)

type opParserFn func(scan.TinyScanner) (bool, string)

type nodifyExpression func(expr ast.Expression)

type nodifyOperator func(tokens []lexer.Token) string

type expressionMaker func(op string, a ast.Expression, b ast.Expression) ast.Expression

var optWS = optionalToken(lexer.TokenWhiteSpace)

var reqWS = requiredToken(lexer.TokenWhiteSpace, nil)

var eofParser = requiredToken(lexer.TokenEOF, nil)

func parseTermExpression() expressionParserFn {
	return func(scanner scan.TinyScanner) (bool, ast.Expression) {
		_, reset := scanner.Mark()
		var expr ast.Expression

		ok, _ := oneOf([]parserFn{
			parseTerm(func(expression ast.Expression) {
				expr = expression
			}),
			parens(lazy(func() parserFn {
				return func(scanner scan.TinyScanner) (bool, interface{}) {
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

// chainl requires at least one expression followed by an optional series of [op expression]
// this combinator is used to eliminate left recursion and build a left-associative expression.
//
// Left recursion:
// e.g. Expression -> Expression + Term
// pseudo code: (would never terminate)
// void Expression() {
//   Expression();
//   match('+');
//   Term();
// }
// left-to-right recursive descent parsers can't handle left recursion and this is just one of several possible
// ways to eliminate left recursion. Though, this particular way doesn't handle indirect left recurision which
// is a series of substitutions that ultimately lead to an infinite recursive call.
//
// Left associativity:
// e.g. (from wikipedia) Consider the expression a ~ b ~ c.
// If the operator ~ has left associativity, this expression would be interpreted as (a ~ b) ~ c.
// If the operator has right associativity, the expression would be interpreted as a ~ (b ~ c).
func chainl(ep expressionParserFn, em expressionMaker, opParser opParserFn) expressionParserFn {
	return func(scanner scan.TinyScanner) (bool, ast.Expression) {
		success, expression := ep(scanner)

		if success {
			for {
				if os, op := opParser(scanner); os {
					if ps, right := ep(scanner); ps {
						expression = em(op, expression, right)
					} else {
						return false, nil
					}
				} else {
					return true, expression
				}
			}
		}

		return false, expression
	}
}

func operatorParser(opParser parserFn, nodifyOperator nodifyOperator) opParserFn {
	return func(scanner scan.TinyScanner) (bool, string) {
		var opText string

		success, _ := required(opParser, func(x []lexer.Token) {
			opText = nodifyOperator(x)
		})(scanner)

		return success, opText
	}
}

func comparison() opParserFn {
	return operatorParser(operator(`=`), func(tokens []lexer.Token) string {
		return tokens[1].Text
	})
}

func logical() opParserFn {
	return operatorParser(oneOf([]parserFn{
		operator(`AND`),
		operator(`OR`),
	}, nil), func(tokens []lexer.Token) string {
		return tokens[1].Text
	})
}

func mult() opParserFn {
	return operatorParser(oneOf([]parserFn{
		operator(`\*`),
		operator(`/`),
	}, nil), func(tokens []lexer.Token) string {
		return tokens[1].Text
	})
}

func sum() opParserFn {
	return operatorParser(oneOf([]parserFn{
		operator(`\+`),
		operator(`-`),
	}, nil), func(tokens []lexer.Token) string {
		return tokens[1].Text
	})
}

func parseExpression() expressionParserFn {
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

func parseTerm(nodify nodifyExpression) parserFn {
	return oneOf([]parserFn{
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

func optionalToken(expected lexer.Kind) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
		next := scanner.Peek()
		if next.Kind == expected {
			scanner.Next()
		}

		return true, nil
	}
}

func ident(n func(string)) parserFn {
	return requiredToken(lexer.TokenIdentifier, func(tokens []lexer.Token) {
		n(tokens[0].Text)
	})
}

func token(expected lexer.Kind) parserFn {
	return requiredToken(expected, nil)
}

func requiredToken(expected lexer.Kind, nodify nodify) parserFn {
	return required(func(scanner scan.TinyScanner) (bool, interface{}) {
		next := scanner.Next()
		if next.Kind == expected {
			return true, nil
		}

		return false, nil
	}, nodify)
}

func operator(operatorText string) parserFn {
	return all([]parserFn{
		optWS,
		regex(operatorText),
		optWS,
	}, nil)
}

func parens(inner parserFn) parserFn {
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

func parensCommaSep(p parserFn) parserFn {
	return parens(commaSeparated(p))
}

func commaSeparated(p parserFn) parserFn {
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

func keyword(t lexer.Kind) parserFn {
	return allX(
		optWS,
		token(t),
		oneOf([]parserFn{eofParser, optWS}, nil), // Should this be required white space?
	)
}

func makeExpressionParser(nodify nodifyExpression) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
		success, expr := parseExpression()(scanner)

		if success {
			nodify(expr)
		}

		return success, expr
	}
}
