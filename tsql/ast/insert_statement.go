package ast

type ValueSet map[string]Expression

// InsertStatement represents an instruction to insert data into a table and expressions that evaluate to values
type InsertStatement struct {
	Table     string
	Values    ValueSet
	Returning []string
}

func (*InsertStatement) iStatement() {}

func (*InsertStatement) Mutates() bool { return true }

func (s *InsertStatement) ReturnsRows() bool { return len(s.Returning) > 0 }
