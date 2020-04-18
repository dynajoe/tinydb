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
	Text() string
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
func (s *tinyScanner) Reset() {
	s.position = 0
	s.committed = ""
	s.isAborted = false
}

func (s *tinyScanner) Text() string {
	return s.input
}

func (s *tinyScanner) Range(start int, end int) []TinyDBItem {
	return s.items[start:end]
}

func (s *tinyScanner) Mark() (int, func()) {
	position := s.position
	committed := s.committed
	return s.position, func() {
		s.position = position
		s.committed = committed
	}
}

func (s *tinyScanner) Peek() TinyDBItem {
	token := s.Next()

	if s.position >= 1 {
		s.Backup()
	}

	return token
}

func (s *tinyScanner) Backup() {
	if s.position > 0 {
		s.position--
	} else {
		panic("Attempting to back up before any tokens")
	}
}

func (s *tinyScanner) Pos() int {
	return s.position
}

func (s *tinyScanner) Info() {
	fmt.Printf("context: %s\n\tposition: %d\n\titems: %s\n\n",
		s.committed,
		s.position,
		s.items)
}

func (s *tinyScanner) Next() TinyDBItem {
	if s.isAborted {
		return TinyDBItem{
			Token:    tsqlEOF,
			Position: 0,
			Text:     "",
		}
	}

	var token TinyDBItem

	if s.position >= len(s.items) {
		token = s.lexer.nextItem()
		s.items = append(s.items, token)
	} else {
		token = s.items[s.position]
	}

	s.position++

	return token
}

func (s *tinyScanner) Commit(landmark string) {
	s.committed = landmark
}
