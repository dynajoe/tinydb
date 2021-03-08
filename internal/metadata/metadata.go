package metadata

import (
	"fmt"
	"reflect"

	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/tsql"
	"github.com/joeandaverde/tinydb/tsql/ast"
)

// ColumnDefinition represents a specification for a column in a table
type ColumnDefinition struct {
	Name         string          `json:"name"`
	Type         storage.SQLType `json:"type"`
	Offset       int             `json:"offset"`
	PrimaryKey   bool            `json:"is_primary_key"`
	DefaultValue interface{}     `json:"default_value"`
}

type TableDefinition struct {
	Name     string              `json:"name"`
	RawText  string              `json:"raw_text"`
	Columns  []*ColumnDefinition `json:"columns"`
	RootPage int                 `json:"root_page"`
}

var tableCache = make(map[string]*TableDefinition)

func GetTableDefinition(pager storage.Pager, name string) (*TableDefinition, error) {
	if tableDefinition, ok := tableCache[name]; ok {
		return tableDefinition, nil
	}

	cursor, err := storage.NewCursor(pager, storage.CURSOR_READ, 1, name)
	if err != nil {
		return nil, err
	}

	hasMore, err := cursor.Rewind()
	if err != nil {
		return nil, err
	}

	for hasMore {
		record, err := cursor.CurrentCell()
		if err != nil {
			return nil, err
		}

		if name == record.Fields[1].Data.(string) {
			tableDefinition, err := tableDefinitionFromRecord(record)
			if err != nil {
				return nil, err
			}
			tableCache[name] = tableDefinition
			return tableDefinition, nil
		}

		hasMore, err = cursor.Next()
		if err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf("table not found: %s", name)
}

func tableDefinitionFromRecord(record *storage.Record) (*TableDefinition, error) {
	createSQL := record.Fields[4].Data.(string)
	stmt, err := tsql.Parse(createSQL)
	if err != nil {
		return nil, err
	}
	var cols []*ColumnDefinition
	for i, c := range stmt.(*ast.CreateTableStatement).Columns {
		cols = append(cols, &ColumnDefinition{
			Offset:     i,
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
		Name:     record.Fields[1].Data.(string),
		RootPage: rootPage,
		Columns:  cols,
	}, nil
}
