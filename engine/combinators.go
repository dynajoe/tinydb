package engine

import (
	"regexp"
	"strings"
)

func chainl(expressionParser ExpressionParser, expressionMaker ExpressionMaker, opParser OperatorParser) ExpressionParser {
	return func(scanner *TSQLScanner) (bool, Expression) {
		success, expression := expressionParser(scanner)

		if success {
			for {
				if os, op := opParser(scanner); os {
					if ps, right := expressionParser(scanner); ps {
						expression = expressionMaker(op, expression, right)
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
	return func(scanner *TSQLScanner) (bool, interface{}) {
		return x()(scanner)
	}
}

func text(r string) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		next := scanner.peek()

		if strings.ToLower(r) == strings.ToLower(next.text) {
			scanner.next()
			return true, r
		}

		return false, nil
	}
}

func regex(r string) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		next := scanner.peek()

		if regexp.MustCompile(r).MatchString(next.text) {
			scanner.next()
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
	return func(scanner *TSQLScanner) (bool, interface{}) {
		var results []interface{}

		for {
			if success, result := parser(scanner); success {
				results = append(results, result)
			} else {
				break
			}
		}

		return true, results
	}
}

func all(parsers []Parser, nodify NodifyMany) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		start := scanner.position
		matchesAll := true
		var tokens [][]item

		for _, parser := range parsers {
			before := scanner.position

			if success, _ := parser(scanner); !success {
				matchesAll = false
				break
			}

			tokens = append(tokens, scanner.items[before:scanner.position])
		}

		if !matchesAll {
			scanner.position = start
		} else if nodify != nil {
			nodify(tokens)
		}

		return matchesAll, tokens
	}
}

func oneOf(parsers []Parser, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		start := scanner.position

		for _, parser := range parsers {
			if success, result := parser(scanner); success {
				token := scanner.items[start:scanner.position]

				if nodify != nil {
					nodify(token)
				}

				return true, result
			}

			scanner.position = start
		}

		return false, nil
	}
}

func optional(parser Parser, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		start := scanner.position

		if success, _ := parser(scanner); success {
			token := scanner.items[start:scanner.position]

			if nodify != nil {
				nodify(token)
			}

			return true, token
		}

		scanner.position = start
		return true, nil
	}
}

func required(parser Parser, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		start := scanner.position

		if success, result := parser(scanner); success {
			token := scanner.items[start:scanner.position]

			if nodify != nil {
				nodify(token)
			}

			return true, result
		}

		scanner.position = start
		return false, nil
	}
}
