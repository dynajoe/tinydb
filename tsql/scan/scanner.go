package scan

import (
	"fmt"

	"github.com/joeandaverde/tinydb/tsql/lexer"
)

type TinyScanner interface {
	Peek() lexer.Token
	Backup()
	Info()
	Next() lexer.Token
	Commit(landmark string)
	Committed() string
	Pos() int
	Mark() (int, func())
	Range(int, int) []lexer.Token
	Reset()
	Text() string
}

// NewScanner returns a new TinyScanner to navigate the tokens from the input.
func NewScanner(input string) TinyScanner {
	sqlLexer := lexer.NewLexer(input)
	return &tinyScanner{
		tokens:   sqlLexer.Exec(),
		input:    input,
		items:    []lexer.Token{},
		position: 0,
	}
}

type tinyScanner struct {
	tokens    <-chan lexer.Token
	input     string
	items     []lexer.Token
	position  int
	committed string
}

// Start resets the scanner to the start
func (s *tinyScanner) Reset() {
	s.position = 0
	s.committed = ""
}

func (s *tinyScanner) Text() string {
	return s.input
}

func (s *tinyScanner) Committed() string {
	return s.committed
}

func (s *tinyScanner) Range(start int, end int) []lexer.Token {
	return s.items[start:end]
}

// Mark returns the position of the scanner and a function
// to reset the scanner back to the position
func (s *tinyScanner) Mark() (int, func()) {
	position := s.position
	committed := s.committed
	return s.position, func() {
		s.position = position
		s.committed = committed
	}
}

func (s *tinyScanner) Peek() lexer.Token {
	token := s.Next()

	if s.position >= 1 {
		s.Backup()
	}

	return token
}

func (s *tinyScanner) Backup() {
	if s.position == 0 {
		return
	}
	s.position--
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

func (s *tinyScanner) Next() lexer.Token {
	var token lexer.Token

	if s.position >= len(s.items) {
		token = <-s.tokens
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
