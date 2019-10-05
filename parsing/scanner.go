package parsing

import "fmt"

type TSQLScanner struct {
	lexer     *tsqlLexer
	input     string
	items     []item
	position  int
	isAborted bool
	committed string
}

func (scanner *TSQLScanner) peek() item {
	token := scanner.next()

	if scanner.position >= 1 {
		scanner.backup()
	}

	return token
}

func (scanner *TSQLScanner) backup() {
	if scanner.position > 0 {
		scanner.position--
	} else {
		panic("Attempting to back up before any tokens")
	}
}

func (scanner *TSQLScanner) info() {
	fmt.Printf("context: %s\n\tposition: %d\n\titems: %s\n\n",
		scanner.committed,
		scanner.position,
		scanner.items)
}

func (scanner *TSQLScanner) start(parser Parser) (bool, interface{}) {
	scanner.position = 0
	scanner.committed = ""
	scanner.isAborted = false

	success, result := scanner.run(parser)

	return success, result
}

func (scanner *TSQLScanner) run(parser Parser) (bool, interface{}) {
	start := scanner.position

	if success, result := parser(scanner); success {
		return true, result
	}

	scanner.position = start
	return false, nil
}

func (scanner *TSQLScanner) next() item {
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
