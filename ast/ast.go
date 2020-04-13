package ast

import (
	"fmt"
)

// Statement represents a TinySQL Statement
type Statement interface {
	iStatement()
}

// Expression represents an TinySQL Expression that can be evaluated
type Expression interface {
	Evaluate(ctx EvaluationContext) EvaluatedExpression
}

// EvaluationContext provides a means for resolving identifiers to values
type EvaluationContext interface {
	GetValue(ident *Ident) (interface{}, bool)
}

// ColumnReference represents an instruction to create a table
type ColumnReference struct {
	Table string
	Alias string
	Index int
}

// CreateTableStatement represents an instruction to create a table
type CreateTableStatement struct {
	TableName   string
	IfNotExists bool
	Columns     []ColumnDefinition
	RawText     string
}

// ColumnDefinition represents a specification for a column in a table
type ColumnDefinition struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"is_primary_key"`
}

// TableAlias represents a local name and the table it refers to
type TableAlias struct {
	Name  string
	Alias string
}

// SelectStatement represents an instruction to select/filter rows from one or more tables
type SelectStatement struct {
	From    []TableAlias
	Columns []string
	Filter  Expression
}

type ValueSet map[string]Expression

// InsertStatement represents an instruction to insert data into a table and expressions that evaluate to values
type InsertStatement struct {
	Table     string
	Values    ValueSet
	Returning []string
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
	Value     string
	TokenType Token
}

func (*SelectStatement) iStatement()      {}
func (*InsertStatement) iStatement()      {}
func (*CreateTableStatement) iStatement() {}

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

func (s *SelectStatement) String() string {
	return fmt.Sprintf("SELECT %s\nFROM %s\nWHERE %s", s.Columns, s.From, s.Filter)
}

func (o *BinaryOperation) String() string {
	return fmt.Sprintf("(%s %s %s)", o.Left, o.Operator, o.Right)
}
