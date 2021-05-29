package ast

// Statement represents a TinySQL Statement
type Statement interface {
	Mutates() bool
	ReturnsRows() bool
	iStatement()
}
