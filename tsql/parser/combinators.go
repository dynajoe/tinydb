package parser

import (
	"regexp"
	"strings"

	"github.com/joeandaverde/tinydb/tsql/lexer"
	"github.com/joeandaverde/tinydb/tsql/scan"
)

type parserFn func(scan.TinyScanner) (bool, interface{})

type nodify func(tokens []lexer.Token)

type nodifyMany func(tokens [][]lexer.Token)

// lazy calls a parser producing function each time it's invoked.
// this combinator is useful when a parser refers to itself
func lazy(x func() parserFn) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
		return x()(scanner)
	}
}

// text parses a string using case-insensitive comparison
func text(r string) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
		next := scanner.Peek()

		if strings.EqualFold(r, next.Text) {
			scanner.Next()
			return true, r
		}

		return false, nil
	}
}

// regex constructs a regex from the provided string and
// continues if it matches the next token
func regex(r string) parserFn {
	regex := regexp.MustCompile(r)
	return func(scanner scan.TinyScanner) (bool, interface{}) {
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
func separatedBy1(separator parserFn, parser parserFn) parserFn {
	return all([]parserFn{
		parser,
		zeroOrMore(all([]parserFn{
			separator,
			parser,
		}, nil)),
	}, nil)
}

// zeroOrMore runs parser until it doesn't match anymore and always succeeds
func zeroOrMore(parser parserFn) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
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
func allX(parsers ...parserFn) parserFn {
	return all(parsers, nil)
}

// all requires that all parsers succeed or no input in consumed
func all(parsers []parserFn, nodify nodifyMany) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
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
func oneOf(parsers []parserFn, nodify nodify) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
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
func optionalX(parser parserFn) parserFn {
	return optional(parser, nil)
}

// optional always succeeds and may consume input if the parser succeeds.
func optional(parser parserFn, nodify nodify) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
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
func required(parser parserFn, nodify nodify) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
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

func committed(committedAt string, p parserFn) parserFn {
	return func(scanner scan.TinyScanner) (bool, interface{}) {
		scanner.Commit(committedAt)
		_, reset := scanner.Mark()

		if success, results := p(scanner); success {
			return success, results
		}

		reset()
		return false, nil
	}
}
