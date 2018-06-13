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
	tsqlOpenParen
	tsqlCloseParen
	tsqlAsterisk

	tsqlIdentifier

	tsqlSelect
	tsqlFrom
	tsqlInsert
	tsqlCreate
	tsqlTable
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
		return "Error"
	}
	return fmt.Sprintf("[%s]", i.text)
}

const eof = -1

type tsqlLexer struct {
	state stateFn
	name  string
	input string
	start int
	pos   int
	width int
	items chan item
}

type stateFn func(*tsqlLexer) stateFn

func lex(name string, input string) *tsqlLexer {
	l := &tsqlLexer{
		state: lexTinySQL,
		name:  name,
		input: input,
		items: make(chan item),
	}

	go l.run()

	return l
}

func lexWhiteSpace(l *tsqlLexer) stateFn {
	for isWhiteSpace(l.peek()) {
		l.next()
	}

	l.emit(tsqlWhiteSpace)

	return lexTinySQL
}

func lexAlphaNumeric(l *tsqlLexer) stateFn {
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
			} else if strings.ToUpper(value) == "TABLE" {
				l.emit(tsqlTable)
			} else if strings.ToUpper(value) == "CREATE" {
				l.emit(tsqlCreate)
			} else {
				l.emit(tsqlIdentifier)
			}

			return lexTinySQL
		}
	}
}

func lexTinySQL(l *tsqlLexer) stateFn {
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
		} else if r == '(' {
			l.next()
			l.emit(tsqlOpenParen)
			return lexTinySQL
		} else if r == ')' {
			l.next()
			l.emit(tsqlCloseParen)
			return lexTinySQL
		} else if r == ',' {
			l.next()
			l.emit(tsqlComma)
			return lexTinySQL
		} else if r == eof {
			l.emit(tsqlEOF)
		} else {
			return l.errorf("Unexpected token %s", r)
		}
	}

	return nil
}

func (l *tsqlLexer) nextItem() item {
	return <-l.items
}

func (l *tsqlLexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *tsqlLexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}

	r, width := utf8.DecodeRuneInString(l.input[l.pos:])

	l.width = width
	l.pos += l.width

	return r
}

func (l *tsqlLexer) atTerminator() bool {
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

func (l *tsqlLexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{
		tsqlError,
		fmt.Sprintf(format, args...),
		l.start,
	}

	return nil
}

func (l *tsqlLexer) backup() {
	l.pos -= l.width
}

func (l *tsqlLexer) emit(token Token) {
	l.items <- item{token, l.input[l.start:l.pos], l.start}
	l.start = l.pos
}

func (l *tsqlLexer) run() {
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
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}
