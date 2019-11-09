package engine

import (
	"regexp"
	"strings"
)

func chainl(ep expressionParser, em expressionMaker, opParser tsqlOpParser) expressionParser {
	return func(scanner *tsqlScanner) (bool, Expression) {
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

func lazy(x func() tsqlParser) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
		return x()(scanner)
	}
}

func text(r string) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
		next := scanner.peek()

		if strings.ToLower(r) == strings.ToLower(next.text) {
			scanner.next()
			return true, r
		}

		return false, nil
	}
}

func regex(r string) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
		next := scanner.peek()

		if regexp.MustCompile(r).MatchString(next.text) {
			scanner.next()
			return true, r
		}

		return false, nil
	}
}

func separatedBy1(separator tsqlParser, parser tsqlParser) tsqlParser {
	return all([]tsqlParser{
		parser,
		zeroOrMore(all([]tsqlParser{
			separator,
			parser,
		}, nil)),
	}, nil)
}

func zeroOrMore(parser tsqlParser) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
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

func all(parsers []tsqlParser, nodify nodifyMany) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
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

func oneOf(parsers []tsqlParser, nodify nodify) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
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

func optional(parser tsqlParser, nodify nodify) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
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

func required(parser tsqlParser, nodify nodify) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
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
