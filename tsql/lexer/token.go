package lexer

import "fmt"

// Kind the kind of token
type Kind int

const (
	TokenError Kind = iota

	TokenEOF
	TokenWhiteSpace

	TokenComma
	TokenOpenParen
	TokenCloseParen
	TokenAsterisk

	TokenIdentifier

	TokenSelect
	TokenFrom
	TokenWhere
	TokenAs
	TokenIf
	TokenNot
	TokenExists

	TokenCreate
	TokenInsert
	TokenInto
	TokenTable
	TokenValues
	TokenReturning

	TokenEquals
	TokenGt
	TokenLt
	TokenGte
	TokenLte
	TokenNotEq

	TokenAnd
	TokenOr

	TokenPlus
	TokenMinus
	TokenDivide

	TokenString
	TokenNumber
	TokenBoolean
	TokenNull
)

// Token is an output from the lexer
type Token struct {
	Kind     Kind
	Text     string
	Position int
}

func (t Kind) String() string {
	switch {
	case t == TokenEOF:
		return "EOF"
	case t == TokenError:
		return "Error"
	case t == TokenSelect:
		return "SELECT"
	case t == TokenFrom:
		return "FROM"
	case t == TokenWhere:
		return "WHERE"
	}

	return t.String()
}

func (i Token) String() string {
	switch {
	case i.Kind == TokenEOF:
		return "EOF"
	case i.Kind == TokenError:
		return "Error"
	}
	return fmt.Sprintf("[%s]", i.Text)
}