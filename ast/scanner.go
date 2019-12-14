package ast

import "fmt"

type tinyScanner struct {
	lexer     *TinyLexer
	input     string
	items     []TinyDBItem
	position  int
	isAborted bool
	committed string
}

func (scanner *tinyScanner) Peek() TinyDBItem {
	token := scanner.Next()

	if scanner.position >= 1 {
		scanner.Backup()
	}

	return token
}

func (scanner *tinyScanner) Backup() {
	if scanner.position > 0 {
		scanner.position--
	} else {
		panic("Attempting to back up before any tokens")
	}
}

func (scanner *tinyScanner) Info() {
	fmt.Printf("context: %s\n\tposition: %d\n\titems: %s\n\n",
		scanner.committed,
		scanner.position,
		scanner.items)
}

func (scanner *tinyScanner) Next() TinyDBItem {
	if scanner.isAborted {
		return TinyDBItem{
			Token:    tsqlEOF,
			Position: 0,
			Text:     "",
		}
	}

	var token TinyDBItem

	if scanner.position >= len(scanner.items) {
		token = scanner.lexer.nextItem()
		scanner.items = append(scanner.items, token)
	} else {
		token = scanner.items[scanner.position]
	}

	scanner.position++

	return token
}

func (scanner *tinyScanner) start(parser tsqlParser) (bool, interface{}) {
	scanner.position = 0
	scanner.committed = ""
	scanner.isAborted = false

	success, result := scanner.run(parser)

	return success, result
}

func (scanner *tinyScanner) run(parser tsqlParser) (bool, interface{}) {
	start := scanner.position

	if success, result := parser(scanner); success {
		return true, result
	}

	scanner.position = start
	return false, nil
}
