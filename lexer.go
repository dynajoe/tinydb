package main

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Token - A TinySQL Token
type Token int

const (
	tsqlError Token = iota

	tsqlEOF
	tsqlWhiteSpace

	tsqlComma
	tsqlAsterisk

	tsqlSelect
	tsqlFrom
	tsqlIdentifier
)

type item struct {
	token    Token
	text     string
	position int
}

func (i item) String() string {
	switch {
	case i.token == tsqlEOF:
		return "EOF"
	case i.token == tsqlError:
		return i.text
	}
	return fmt.Sprintf(" %d [%d] %s ", i.token, i.position, i.text)
}

const eof = -1

type stateFn func(*lexer) stateFn

type lexer struct {
	state stateFn
	name  string
	input string
	start int
	pos   int
	width int
	items chan item
}

func lex(name string, input string) *lexer {
	l := &lexer{
		state: lexTinySQL,
		name:  name,
		input: input,
		items: make(chan item, 2),
	}

	go l.run()

	return l
}

func (l *lexer) nextItem() item {
	return <-l.items
}

func lexWhiteSpace(l *lexer) stateFn {
	for isWhiteSpace(l.peek()) {
		l.next()
	}

	l.emit(tsqlWhiteSpace)

	return lexTinySQL
}

func lexAlphaNumeric(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case isAlphaNumeric(r):
		default:
			l.backup()

			value := l.input[l.start:l.pos]

			if strings.ToUpper(value) == "SELECT" {
				l.emit(tsqlSelect)
			} else if strings.ToUpper(value) == "FROM" {
				l.emit(tsqlFrom)
			} else {
				l.emit(tsqlIdentifier)
			}

			return lexTinySQL
		}
	}
}

func lexTinySQL(l *lexer) stateFn {
	for {
		r := l.peek()

		if isWhiteSpace(r) {
			return lexWhiteSpace(l)
		} else if isAlphaNumeric(r) {
			return lexAlphaNumeric(l)
		} else if r == '*' {
			l.next()
			l.emit(tsqlAsterisk)
			return lexTinySQL
		} else if r == eof {
			l.emit(tsqlEOF)
		} else {
			return l.errorf("Unexpected token %s", r)
		}
	}

	return nil
}

func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *lexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}

	r, width := utf8.DecodeRuneInString(l.input[l.pos:])

	l.width = width
	l.pos += l.width

	return r
}

func (l *lexer) atTerminator() bool {
	r := l.peek()

	if isWhiteSpace(r) || isEndOfLine(r) {
		return true
	}

	switch r {
	case eof, '.', ',', '|', ':', ')', '(':
		return true
	}

	return false
}

func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{
		tsqlError,
		fmt.Sprintf(format, args...),
		l.start,
	}

	return nil
}

func (l *lexer) backup() {
	l.pos -= l.width
}

func (l *lexer) emit(token Token) {
	l.items <- item{token, l.input[l.start:l.pos], l.start}
	l.start = l.pos
}

func (l *lexer) run() {
	for state := lexTinySQL; state != nil; {
		state = state(l)
	}
	close(l.items)
}

func isAlphaNumeric(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func isEndOfLine(r rune) bool {
	return r == '\n' || r == '\r' || r == eof
}

func isWhiteSpace(r rune) bool {
	return r == ' ' || r == '\n' || r == '\t'
}
