package interpret

import (
	"github.com/joeandaverde/tinydb/engine"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/tsql/ast"
)

func createTable(e *engine.Engine, createStatement *ast.CreateTableStatement) error {
	var columnDefinitions []engine.ColumnDefinition
	for i, c := range createStatement.Columns {
		columnDefinitions = append(columnDefinitions, engine.ColumnDefinition{
			Name:       c.Name,
			Type:       storage.SQLTypeFromString(c.Type),
			Offset:     i,
			PrimaryKey: c.PrimaryKey,
		})
	}

	tableMetadata := &engine.TableDefinition{
		Name:    createStatement.TableName,
		Columns: columnDefinitions,
		RawText: createStatement.RawText,
	}

	return e.CreateTable(tableMetadata)
}
