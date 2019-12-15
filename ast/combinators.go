package ast

import (
	"regexp"
	"strings"
)

func chainl(ep ExpressionParser, em expressionMaker, opParser tsqlOpParser) ExpressionParser {
	return func(scanner TinyScanner) (bool, Expression) {
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

func lazy(x func() Parser) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		return x()(scanner)
	}
}

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

func regex(r string) Parser {
	regex := regexp.MustCompile(r)
	return func(scanner TinyScanner) (bool, interface{}) {
		next := scanner.Peek()

		if regex.MatchString(next.Text) {
			scanner.Next()
			return true, r
		}

		return false, nil
	}
}

func separatedBy1(separator Parser, parser Parser) Parser {
	return all([]Parser{
		parser,
		zeroOrMore(all([]Parser{
			separator,
			parser,
		}, nil)),
	}, nil)
}

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

func allX(parsers ...Parser) Parser {
	return all(parsers, nil)
}

func all(parsers []Parser, nodify nodifyMany) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		_, reset := scanner.Mark()
		matchesAll := true
		var tokens [][]TinyDBItem

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

func optionalX(parser Parser) Parser {
	return optional(parser, nil)
}

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
