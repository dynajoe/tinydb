package ast

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
	tsqlIf
	tsqlNot
	tsqlExists

	tsqlCreate
	tsqlInsert
	tsqlInto
	tsqlTable
	tsqlValues
	tsqlReturning

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

type TinyDBItem struct {
	Token    Token
	Text     string
	Position int
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

func (i TinyDBItem) String() string {
	switch {
	case i.Token == tsqlEOF:
		return "EOF"
	case i.Token == tsqlError:
		return "Error"
	}
	return fmt.Sprintf("[%s]", i.Text)
}

const eof = -1

type TinyLexer struct {
	Items     chan TinyDBItem
	state     stateFn
	input     string
	remaining string
	start     int
	pos       int
	width     int
}

type stateFn func(*TinyLexer) stateFn

func NewLexer(input string) *TinyLexer {
	l := &TinyLexer{
		state: lexTinySQL,
		input: input,
		Items: make(chan TinyDBItem),
	}

	go l.run()

	return l
}

func lexWhiteSpace(l *TinyLexer) stateFn {
	for isWhiteSpace(l.peek()) {
		l.next()
	}

	l.emit(tsqlWhiteSpace)

	return lexTinySQL
}

func lexNumber(l *TinyLexer) stateFn {
	for unicode.IsDigit(l.peek()) {
		l.next()
	}

	l.emit(tsqlNumber)

	return lexTinySQL
}

func lexAlphaNumeric(l *TinyLexer) stateFn {
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
		} else if strings.ToUpper(value) == "IF" {
			l.emit(tsqlIf)
		} else if strings.ToUpper(value) == "NOT" {
			l.emit(tsqlNot)
		} else if strings.ToUpper(value) == "EXISTS" {
			l.emit(tsqlExists)
		} else if strings.ToUpper(value) == "RETURNING" {
			l.emit(tsqlReturning)
		} else if strings.ToUpper(value) == "VALUES" {
			l.emit(tsqlValues)
		} else if strings.ToUpper(value) == "TRUE" || strings.ToUpper(value) == "FALSE" {
			l.emit(tsqlBoolean)
		} else {
			l.emit(tsqlIdentifier)
		}

		return lexTinySQL
	}
}

func lexSymbol(l *TinyLexer) stateFn {
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

func lexString(l *TinyLexer) stateFn {
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
				panic("Non terminated string Token!")
			}

			previous = current
		}

		return lexTinySQL
	}

	return nil
}

func lexTinySQL(l *TinyLexer) stateFn {
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
		return l.errorf("Unexpected Token %s", string(r))
	}

	return nil
}

func (l *TinyLexer) nextItem() TinyDBItem {
	return <-l.Items
}

func (l *TinyLexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *TinyLexer) peek2() rune {
	if l.peek() == eof {
		return eof
	}

	l.next()
	r := l.peek()
	l.backup()

	return r
}

func (l *TinyLexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}

	r, width := utf8.DecodeRuneInString(l.input[l.pos:])

	l.width = width
	l.pos += l.width

	return r
}

func (l *TinyLexer) atTerminator() bool {
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

func (l *TinyLexer) errorf(format string, args ...interface{}) stateFn {
	l.Items <- TinyDBItem{
		tsqlError,
		fmt.Sprintf(format, args...),
		l.start,
	}

	return nil
}

func (l *TinyLexer) backup() {
	l.pos -= l.width
}

func (l *TinyLexer) emit(token Token) {
	l.Items <- TinyDBItem{token, l.input[l.start:l.pos], l.start}
	l.remaining = l.input[l.pos:]
	l.start = l.pos
}

func (l *TinyLexer) run() {
	for state := lexTinySQL; state != nil; {
		state = state(l)
	}
	close(l.Items)
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
