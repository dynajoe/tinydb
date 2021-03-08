package lexer

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const eof = -1

type stateFn func(*Lexer) stateFn

// Lexer produces tokens from input
type Lexer struct {
	items     chan Token
	state     stateFn
	input     string
	remaining string
	start     int
	pos       int
	width     int
}

// NewLexer initializes a lexer with input.
func NewLexer(input string) *Lexer {
	return &Lexer{
		state: lexTinySQL,
		input: input,
		items: make(chan Token),
	}
}

// Exec starts producing tokens
func (l *Lexer) Exec() <-chan Token {
	go func() {
		defer close(l.items)
		for state := lexTinySQL; state != nil; {
			state = state(l)
		}
	}()

	return l.items
}

func lexWhiteSpace(l *Lexer) stateFn {
	for isWhiteSpace(l.peek()) {
		l.next()
	}

	l.emit(TokenWhiteSpace)

	return lexTinySQL
}

func lexNumber(l *Lexer) stateFn {
	for unicode.IsDigit(l.peek()) {
		l.next()
	}

	l.emit(TokenNumber)

	return lexTinySQL
}

func lexAlphaNumeric(l *Lexer) stateFn {
	for {
		r := l.next()

		if isAlphaNumeric(r) {
			continue
		}

		l.backup()

		value := l.input[l.start:l.pos]

		if strings.ToUpper(value) == "SELECT" {
			l.emit(TokenSelect)
		} else if strings.ToUpper(value) == "FROM" {
			l.emit(TokenFrom)
		} else if strings.ToUpper(value) == "AS" {
			l.emit(TokenAs)
		} else if strings.ToUpper(value) == "TABLE" {
			l.emit(TokenTable)
		} else if strings.ToUpper(value) == "WHERE" {
			l.emit(TokenWhere)
		} else if strings.ToUpper(value) == "AND" {
			l.emit(TokenAnd)
		} else if strings.ToUpper(value) == "OR" {
			l.emit(TokenOr)
		} else if strings.ToUpper(value) == "CREATE" {
			l.emit(TokenCreate)
		} else if strings.ToUpper(value) == "INSERT" {
			l.emit(TokenInsert)
		} else if strings.ToUpper(value) == "VALUES" {
			l.emit(TokenValues)
		} else if strings.ToUpper(value) == "INTO" {
			l.emit(TokenInto)
		} else if strings.ToUpper(value) == "IF" {
			l.emit(TokenIf)
		} else if strings.ToUpper(value) == "NOT" {
			l.emit(TokenNot)
		} else if strings.ToUpper(value) == "EXISTS" {
			l.emit(TokenExists)
		} else if strings.ToUpper(value) == "RETURNING" {
			l.emit(TokenReturning)
		} else if strings.ToUpper(value) == "VALUES" {
			l.emit(TokenValues)
		} else if strings.ToUpper(value) == "TRUE" || strings.ToUpper(value) == "FALSE" {
			l.emit(TokenBoolean)
		} else if strings.ToUpper(value) == "BEGIN" {
			l.emit(TokenBegin)
		} else if strings.ToUpper(value) == "COMMIT" {
			l.emit(TokenCommit)
		} else if strings.ToUpper(value) == "ROLLBACK" {
			l.emit(TokenRollback)
		} else if strings.ToUpper(value) == "NULL" {
			l.emit(TokenNull)
		} else {
			l.emit(TokenIdentifier)
		}

		return lexTinySQL
	}
}

func lexSymbol(l *Lexer) stateFn {
	switch r := l.peek(); r {
	case '>':
		l.next()

		if l.next() == '=' {
			l.emit(TokenGte)
		} else {
			l.backup()
			l.emit(TokenGt)
		}
	case '<':
		l.next()

		if l.next() == '=' {
			l.emit(TokenLte)
		} else {
			l.backup()
			l.emit(TokenLt)
		}
	case '=':
		l.next()
		l.emit(TokenEquals)
	case '!':
		if l.peek2() == '=' {
			l.next()
			l.next()
			l.emit(TokenNotEq)
		}
	case '*':
		l.next()
		l.emit(TokenAsterisk)
	case '+':
		l.next()
		l.emit(TokenPlus)
	case '-':
		l.next()
		l.emit(TokenMinus)
	case '/':
		l.next()
		l.emit(TokenDivide)
	case '(':
		l.next()
		l.emit(TokenOpenParen)
	case ')':
		l.next()
		l.emit(TokenCloseParen)
	case ',':
		l.next()
		l.emit(TokenComma)
	default:
		return nil
	}

	return lexTinySQL
}

func lexString(l *Lexer) stateFn {
	if p := l.peek(); p == '\'' {
		l.next()

		var previous rune
		var current rune

		for {
			current = l.next()

			if current == '\'' && previous != '\'' {
				l.emit(TokenString)
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

func lexTinySQL(l *Lexer) stateFn {
	r := l.peek()

	if r == eof {
		l.emit(TokenEOF)
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

func (l *Lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *Lexer) peek2() rune {
	if l.peek() == eof {
		return eof
	}

	l.next()
	r := l.peek()
	l.backup()

	return r
}

func (l *Lexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}

	r, width := utf8.DecodeRuneInString(l.input[l.pos:])

	l.width = width
	l.pos += l.width

	return r
}

func (l *Lexer) atTerminator() bool {
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

func (l *Lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- Token{
		Kind:     TokenError,
		Text:     fmt.Sprintf(format, args...),
		Position: l.start,
	}

	return nil
}

func (l *Lexer) backup() {
	l.pos -= l.width
}

func (l *Lexer) emit(kind Kind) {
	l.items <- Token{
		Kind:     kind,
		Text:     l.input[l.start:l.pos],
		Position: l.start,
	}
	l.remaining = l.input[l.pos:]
	l.start = l.pos
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
