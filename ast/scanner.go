package ast

import (
	"fmt"
)

type SuspendFunc func(s TinyScanner) bool

type TinyScanner interface {
	Peek() TinyDBItem
	Backup()
	Info()
	Next() TinyDBItem
	Commit(landmark string)
	Pos() int
	Mark() (int, func())
	Range(int, int) []TinyDBItem
	Reset()
}

type tinyScanner struct {
	lexer     *TinyLexer
	input     string
	items     []TinyDBItem
	position  int
	isAborted bool
	committed string
}

// Start resets the scanner to the start
func (scanner *tinyScanner) Reset() {
	scanner.position = 0
	scanner.committed = ""
	scanner.isAborted = false
}

func (scanner *tinyScanner) Range(start int, end int) []TinyDBItem {
	return scanner.items[start:end]
}

func (scanner *tinyScanner) Mark() (int, func()) {
	position := scanner.position
	committed := scanner.committed
	return scanner.position, func() {
		scanner.position = position
		scanner.committed = committed
	}
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

func (scanner *tinyScanner) Pos() int {
	return scanner.position
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

func (scanner *tinyScanner) Commit(landmark string) {
	scanner.committed = landmark
}
