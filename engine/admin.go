package engine

import (
	"errors"
	"fmt"
	"reflect"

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
			Type:       storage.SQLTypeFromString(c.Type),
			PrimaryKey: c.PrimaryKey,
		})
	}

	var rootPage int
	switch p := record.Fields[3].Data.(type) {
	case int:
		rootPage = p
	case int64:
		rootPage = int(p)
	case uint:
		rootPage = int(p)
	case uint8:
		rootPage = int(p)
	case uint64:
		rootPage = int(p)
	default:
		panic(fmt.Sprintf("unexpected root page type %v", reflect.TypeOf(record.Fields[3].Data)))
	}
	return &TableDefinition{
		Name:     name,
		RootPage: rootPage,
		Columns:  cols,
	}, nil
}
