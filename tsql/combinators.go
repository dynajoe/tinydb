package tsql

import (
	"regexp"
	"strings"

	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/lexer"
)

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
func chainl(ep ExpressionParser, em expressionMaker, opParser OpParser) ExpressionParser {
	return func(scanner TinyScanner) (bool, ast.Expression) {
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

// lazy calls a parser producing function each time it's invoked.
// this combinator is useful when a parser refers to itself
func lazy(x func() Parser) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		return x()(scanner)
	}
}

// text parses a string using case-insensitive comparison
func text(r string) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		next := scanner.Peek()

		if strings.ToLower(r) == strings.ToLower(next.Text) {
			scanner.Next()
			return true, r
		}

		return false, nil
	}
}

// regex constructs a regex from the provided string and
// continues if it matches the next token
func regex(r string) Parser {
	regex := regexp.MustCompile(r)
	return func(scanner TinyScanner) (bool, interface{}) {
		next := scanner.Peek()

		if regex.MatchString(next.Text) {
			scanner.Next()
			return true, next.Text
		}

		return false, nil
	}
}

// separatedBy1 requires at least one match of [parser] followed by
// zero or more [separator parser].
// this combinator is useful for parsing comma separated lists.
func separatedBy1(separator Parser, parser Parser) Parser {
	return all([]Parser{
		parser,
		zeroOrMore(all([]Parser{
			separator,
			parser,
		}, nil)),
	}, nil)
}

// zeroOrMore runs parser until it doesn't match anymore and always succeeds
func zeroOrMore(parser Parser) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		var results []interface{}

		for {
			_, reset := scanner.Mark()

			if success, result := parser(scanner); success {
				results = append(results, result)
			} else {
				reset()
				break
			}
		}

		return true, results
	}
}

// allX is a less verbose way of writing all([]Parser{...}).
// see all.
func allX(parsers ...Parser) Parser {
	return all(parsers, nil)
}

// all requires that all parsers succeed or no input in consumed
func all(parsers []Parser, nodify nodifyMany) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		_, reset := scanner.Mark()
		matchesAll := true
		var tokens [][]lexer.Token

		for _, parser := range parsers {
			before := scanner.Pos()

			if success, _ := parser(scanner); !success {
				matchesAll = false
				break
			}

			tokens = append(tokens, scanner.Range(before, scanner.Pos()))
		}

		if !matchesAll {
			reset()
		} else if nodify != nil {
			nodify(tokens)
		}

		return matchesAll, tokens
	}
}

// oneOf executes each parser until a success. one parser must succeed.
func oneOf(parsers []Parser, nodify nodify) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		start, reset := scanner.Mark()

		for _, parser := range parsers {
			if success, result := parser(scanner); success {
				token := scanner.Range(start, scanner.Pos())
				if nodify != nil {
					nodify(token)
				}

				return true, result
			}

			reset()
		}

		return false, nil
	}
}

// optionalX a less verbose way to write optional.
// see optional.
func optionalX(parser Parser) Parser {
	return optional(parser, nil)
}

// optional always succeeds and may consume input if the parser succeeds.
func optional(parser Parser, nodify nodify) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		start, reset := scanner.Mark()

		if success, _ := parser(scanner); success {
			token := scanner.Range(start, scanner.Pos())

			if nodify != nil {
				nodify(token)
			}

			return true, token
		}

		reset()
		return true, nil
	}
}

// requires only succeeds if the parser succeeds otherwise,
// no input is consumed and the parser fails.
func required(parser Parser, nodify nodify) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		start, reset := scanner.Mark()

		if success, result := parser(scanner); success {
			token := scanner.Range(start, scanner.Pos())

			if nodify != nil {
				nodify(token)
			}

			return true, result
		}

		reset()
		return false, nil
	}
}
