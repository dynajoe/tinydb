package engine

import (
	"fmt"
)

// Statement represents a TinySQL Statement
type Statement interface {
	iStatement()
}

// Expression represents an TinySQL Expression that can be evaluated
type Expression interface {
	Evaluate([]string, *ExecutionEnvironment) EvaluatedExpression
}

// CreateTableStatement represents an instruction to create a table
type CreateTableStatement struct {
	TableName   string
	IfNotExists bool
	Columns     []ColumnDefinition
}

// ColumnDefinition represents a specification for a column in a table
type ColumnDefinition struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"is_primary_key"`
}

// TableAlias represents a local name and the table it refers to
type TableAlias struct {
	name  string
	alias string
}

// SelectStatement represents an instruction to select/filter rows from one or more tables
type SelectStatement struct {
	From    []TableAlias
	Columns []string
	Filter  Expression
}

// InsertStatement represents an instruction to insert data into a table and expressions that evaluate to values
type InsertStatement struct {
	Table     string
	Values    map[string]Expression
	Returning []string
}

func (*SelectStatement) kind() string      { return "select-statement" }
func (*InsertStatement) kind() string      { return "insert-statement" }
func (*CreateTableStatement) kind() string { return "create-table-statement" }

func (*SelectStatement) iStatement()      {}
func (*InsertStatement) iStatement()      {}
func (*CreateTableStatement) iStatement() {}

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

func (*BinaryOperation) kind() string { return "binary-operation" }
func (*BasicLiteral) kind() string    { return "basic-literal" }
func (*Ident) kind() string           { return "ident" }

func (*BinaryOperation) iExpression() {}
func (*BasicLiteral) iExpression()    {}
func (*Ident) iExpression()           {}

func (s *SelectStatement) String() string {
	return fmt.Sprintf("SELECT %s\nFROM %s\nWHERE %s", s.Columns, s.From, s.Filter)
}

func (o *BinaryOperation) String() string {
	return fmt.Sprintf("(%s %s %s)", o.Left, o.Operator, o.Right)
}
