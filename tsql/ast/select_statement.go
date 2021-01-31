package ast

import "fmt"

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

func (s *SelectStatement) String() string {
	return fmt.Sprintf("SELECT %s\nFROM %s\nWHERE %s", s.Columns, s.From, s.Filter)
}

func (*SelectStatement) iStatement() {}
