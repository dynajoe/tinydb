package engine

import "fmt"

type tsqlScanner struct {
	lexer     *tsqlLexer
	input     string
	items     []item
	position  int
	isAborted bool
	committed string
}

func (scanner *tsqlScanner) peek() item {
	token := scanner.next()

	if scanner.position >= 1 {
		scanner.backup()
	}

	return token
}

func (scanner *tsqlScanner) backup() {
	if scanner.position > 0 {
		scanner.position--
	} else {
		panic("Attempting to back up before any tokens")
	}
}

func (scanner *tsqlScanner) info() {
	fmt.Printf("context: %s\n\tposition: %d\n\titems: %s\n\n",
		scanner.committed,
		scanner.position,
		scanner.items)
}

func (scanner *tsqlScanner) start(parser tsqlParser) (bool, interface{}) {
	scanner.position = 0
	scanner.committed = ""
	scanner.isAborted = false

	success, result := scanner.run(parser)

	return success, result
}

func (scanner *tsqlScanner) run(parser tsqlParser) (bool, interface{}) {
	start := scanner.position

	if success, result := parser(scanner); success {
		return true, result
	}

	scanner.position = start
	return false, nil
}

func (scanner *tsqlScanner) next() item {
	if scanner.isAborted {
		return item{
			token:    tsqlEOF,
			position: 0,
			text:     "",
		}
	}

	var token item

	if scanner.position >= len(scanner.items) {
		token = scanner.lexer.nextItem()
		scanner.items = append(scanner.items, token)
	} else {
		token = scanner.items[scanner.position]
	}

	scanner.position++

	return token
}
