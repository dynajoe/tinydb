package engine

import (
	"fmt"
)

// AST
type Node interface {
	Kind() string
}

type Statement interface {
	Node
	iStatement()
}

type Expression interface {
	Node
	Evaluate([]string, *ExecutionEnvironment) EvaluatedExpression
}

// Statements
type CreateTableStatement struct {
	TableName   string
	IfNotExists bool
	Columns     []ColumnDefinition
}

type ColumnDefinition struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"is_primary_key"`
}

type SelectStatement struct {
	From    map[string]string
	Columns []string
	Filter  Expression
}

type InsertStatement struct {
	Table  string
	Values Values
}

func (*SelectStatement) Kind() string      { return "select-statement" }
func (*InsertStatement) Kind() string      { return "insert-statement" }
func (*CreateTableStatement) Kind() string { return "create-table-statement" }

func (*SelectStatement) iStatement()      {}
func (*InsertStatement) iStatement()      {}
func (*CreateTableStatement) iStatement() {}

// Expressions
type BinaryOperation struct {
	Left     Expression
	Right    Expression
	Operator string
}

type Ident struct {
	Value string
}

type BasicLiteral struct {
	Value     string
	TokenType Token
}

func (*BinaryOperation) Kind() string { return "binary-operation" }
func (*BasicLiteral) Kind() string    { return "basic-literal" }
func (*Ident) Kind() string           { return "ident" }

func (*BinaryOperation) iExpression() {}
func (*BasicLiteral) iExpression()    {}
func (*Ident) iExpression()           {}

// String
func (s *SelectStatement) String() string {
	return fmt.Sprintf("SELECT %s\nFROM %s\nWHERE %s", s.Columns, s.From, s.Filter)
}

func (o *BinaryOperation) String() string {
	return fmt.Sprintf("(%s %s %s)", o.Left, o.Operator, o.Right)
}
