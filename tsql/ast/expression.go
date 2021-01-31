package ast

import (
	"fmt"

	"github.com/joeandaverde/tinydb/tsql/lexer"
)

// Expression represents an TinySQL Expression
type Expression interface {
	iExpression()
}

// BinaryOperation is an expression with two operands
type BinaryOperation struct {
	Left     Expression
	Right    Expression
	Operator string
}

// Ident is a reference to something in the environment
type Ident struct {
	Value string
}

// BasicLiteral represents a string, number, or boolean value
type BasicLiteral struct {
	Value string
	Kind  lexer.Kind
}

func (*BinaryOperation) iExpression() {}
func (*Ident) iExpression()           {}
func (*BasicLiteral) iExpression()    {}

func IdentLiteralOperation(op *BinaryOperation) (*Ident, *BasicLiteral) {
	if leftIdent, rightLiteral := asIdent(op.Left), asLiteral(op.Right); leftIdent != nil && rightLiteral != nil {
		return leftIdent, rightLiteral
	}

	if rightIdent, leftLiteral := asIdent(op.Right), asLiteral(op.Left); rightIdent != nil && leftLiteral != nil {
		return rightIdent, leftLiteral
	}

	return nil, nil
}

func asIdent(e Expression) *Ident {
	if op, ok := e.(*Ident); ok {
		return op
	}

	return nil
}

func asLiteral(e Expression) *BasicLiteral {
	if op, ok := e.(*BasicLiteral); ok {
		return op
	}

	return nil
}

func (o *BinaryOperation) String() string {
	return fmt.Sprintf("(%s %s %s)", o.Left, o.Operator, o.Right)
}
