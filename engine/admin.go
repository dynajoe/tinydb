package engine

import (
	"github.com/joeandaverde/tinydb/internal/storage"
)

func (e *Engine) GetTableDefinition(name string) (TableDefinition, error) {
	pageOne, err := e.Pager.Read(1)
	if err != nil {
		return TableDefinition{}, err
	}

	_ = storage.BTreeFromPage(pageOne)

	return TableDefinition{
		Name: name,
		Columns: []ColumnDefinition{
			{Name: "", Type: "", PrimaryKey: false},
		},
	}, nil
}
