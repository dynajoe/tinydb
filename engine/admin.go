package engine

import (
	"errors"

	"github.com/joeandaverde/tinydb/ast"
	"github.com/joeandaverde/tinydb/internal/btree"
	"github.com/joeandaverde/tinydb/internal/storage"
)

func (e *Engine) GetTableDefinition(name string) (*TableDefinition, error) {
	pageOne, err := e.Pager.Read(1)
	if err != nil {
		return nil, err
	}

	bt := storage.BTreeFromPage(pageOne)
	tableDefinitionItem := bt.Find(&btree.StringItem{Key: name})
	if tableDefinitionItem == nil {
		return nil, errors.New("table not found")
	}
	record := tableDefinitionItem.(*btree.StringItem).Data.(*storage.Record)
	createSQL := record.Fields[4].Data.(string)
	stmt, err := ast.Parse(createSQL)
	if err != nil {
		return nil, err
	}
	var cols []ColumnDefinition
	for _, c := range stmt.(*ast.CreateTableStatement).Columns {
		cols = append(cols, ColumnDefinition{
			Name:       c.Name,
			Type:       c.Type,
			Offset:     0,
			PrimaryKey: false,
		})
	}
	return &TableDefinition{
		Name:    name,
		Columns: cols,
	}, nil
}
