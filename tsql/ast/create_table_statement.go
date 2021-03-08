package ast

// ColumnDefinition represents a specification for a column in a table
type ColumnDefinition struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"is_primary_key"`
}

// CreateTableStatement represents an instruction to create a table
type CreateTableStatement struct {
	TableName   string
	IfNotExists bool
	Columns     []ColumnDefinition
	RawText     string
}

func (*CreateTableStatement) iStatement() {}

func (*CreateTableStatement) Mutates() bool { return true }
