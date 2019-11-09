package engine

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
	tsqlWhere
	tsqlAs

	tsqlCreate
	tsqlInsert
	tsqlInto
	tsqlTable
	tsqlValues

	tsqlEquals
	tsqlGt
	tsqlLt
	tsqlGte
	tsqlLte
	tsqlNotEq

	tsqlAnd
	tsqlOr

	tsqlPlus
	tsqlMinus
	tsqlDivide

	tsqlString
	tsqlNumber
	tsqlBoolean
)

type item struct {
	token    Token
	text     string
	position int
}

func (t Token) String() string {
	switch {
	case t == tsqlEOF:
		return "EOF"
	case t == tsqlError:
		return "Error"
	case t == tsqlSelect:
		return "SELECT"
	case t == tsqlFrom:
		return "FROM"
	case t == tsqlWhere:
		return "WHERE"
	}

	return string(t)
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
	state     stateFn
	name      string
	input     string
	remaining string
	start     int
	pos       int
	width     int
	items     chan item
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

func lexNumber(l *tsqlLexer) stateFn {
	for unicode.IsDigit(l.peek()) {
		l.next()
	}

	l.emit(tsqlNumber)

	return lexTinySQL
}

func lexAlphaNumeric(l *tsqlLexer) stateFn {
	for {
		r := l.next()

		if isAlphaNumeric(r) {
			continue
		}

		l.backup()

		value := l.input[l.start:l.pos]

		if strings.ToUpper(value) == "SELECT" {
			l.emit(tsqlSelect)
		} else if strings.ToUpper(value) == "FROM" {
			l.emit(tsqlFrom)
		} else if strings.ToUpper(value) == "AS" {
			l.emit(tsqlAs)
		} else if strings.ToUpper(value) == "TABLE" {
			l.emit(tsqlTable)
		} else if strings.ToUpper(value) == "WHERE" {
			l.emit(tsqlWhere)
		} else if strings.ToUpper(value) == "AND" {
			l.emit(tsqlAnd)
		} else if strings.ToUpper(value) == "OR" {
			l.emit(tsqlOr)
		} else if strings.ToUpper(value) == "CREATE" {
			l.emit(tsqlCreate)
		} else if strings.ToUpper(value) == "INSERT" {
			l.emit(tsqlInsert)
		} else if strings.ToUpper(value) == "VALUES" {
			l.emit(tsqlValues)
		} else if strings.ToUpper(value) == "INTO" {
			l.emit(tsqlInto)
		} else if strings.ToUpper(value) == "TRUE" || strings.ToUpper(value) == "FALSE" {
			l.emit(tsqlBoolean)
		} else {
			l.emit(tsqlIdentifier)
		}

		return lexTinySQL
	}
}

func lexSymbol(l *tsqlLexer) stateFn {
	switch r := l.peek(); r {
	case '>':
		l.next()

		if l.next() == '=' {
			l.emit(tsqlGte)
		} else {
			l.backup()
			l.emit(tsqlGt)
		}
	case '<':
		l.next()

		if l.next() == '=' {
			l.emit(tsqlLte)
		} else {
			l.backup()
			l.emit(tsqlLt)
		}
	case '=':
		l.next()
		l.emit(tsqlEquals)
	case '!':
		if l.peek2() == '=' {
			l.next()
			l.next()
			l.emit(tsqlNotEq)
		}
	case '*':
		l.next()
		l.emit(tsqlAsterisk)
	case '+':
		l.next()
		l.emit(tsqlPlus)
	case '-':
		l.next()
		l.emit(tsqlMinus)
	case '/':
		l.next()
		l.emit(tsqlDivide)
	case '(':
		l.next()
		l.emit(tsqlOpenParen)
	case ')':
		l.next()
		l.emit(tsqlCloseParen)
	case ',':
		l.next()
		l.emit(tsqlComma)
	default:
		return nil
	}

	return lexTinySQL
}

func lexString(l *tsqlLexer) stateFn {
	if p := l.peek(); p == '\'' {
		l.next()

		var previous rune
		var current rune

		for {
			current = l.next()

			if current == '\'' && previous != '\'' {
				l.emit(tsqlString)
				break
			} else if current == eof {
				panic("Non terminated string token!")
			}

			previous = current
		}

		return lexTinySQL
	}

	return nil
}

func lexTinySQL(l *tsqlLexer) stateFn {
	r := l.peek()

	if r == eof {
		l.emit(tsqlEOF)
	} else if isWhiteSpace(r) {
		return lexWhiteSpace(l)
	} else if resume := lexSymbol(l); resume != nil {
		return resume
	} else if resume := lexString(l); resume != nil {
		return resume
	} else if unicode.IsDigit(r) {
		return lexNumber(l)
	} else if isAlphaNumeric(r) {
		return lexAlphaNumeric(l)
	} else {
		return l.errorf("Unexpected token %s", string(r))
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

func (l *tsqlLexer) peek2() rune {
	if l.peek() == eof {
		return eof
	}

	l.next()
	r := l.peek()
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
	l.remaining = l.input[l.pos:]
	l.start = l.pos
}

func (l *tsqlLexer) run() {
	for state := lexTinySQL; state != nil; {
		state = state(l)
	}
	close(l.items)
}

func isAlphaNumeric(r rune) bool {
	return r == '_' || r == '.' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func isEndOfLine(r rune) bool {
	return r == '\n' || r == '\r' || r == eof
}

func isWhiteSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}
