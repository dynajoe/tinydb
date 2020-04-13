package engine

import (
	"github.com/joeandaverde/tinydb/ast"
	"github.com/joeandaverde/tinydb/internal/storage"
)

func createTable(engine *Engine, createStatement *ast.CreateTableStatement) (*TableDefinition, error) {
	//tablePath := filepath.Join(engine.Config.DataDir, strings.ToLower(createStatement.TableName))

	// // TODO: lookup in master table
	// if _, err := os.Stat(tablePath); !createStatement.IfNotExists && !os.IsNotExist(err) {
	// 	return nil, fmt.Errorf("table already exists")
	// }

	tableRecord := storage.NewRecord([]storage.Field{
		{
			Type: storage.Text,
			// type: text
			Data: "table",
		},
		{
			Type: storage.Text,
			// name: text
			Data: createStatement.TableName,
		},
		{
			Type: storage.Text,
			// tablename: text
			Data: createStatement.TableName,
		},
		{
			Type: storage.Byte,
			// rootpage: integer
			Data: 1,
		},
		{
			Type: storage.Text,
			// sql: text
			Data: createStatement.RawText,
		},
	})

	// Update Page 1
	pageOne, err := engine.Pager.Read(1)
	if err != nil {
		return nil, err
	}
	if err := storage.WriteRecord(pageOne, tableRecord); err != nil {
		return nil, err
	}
	if err := engine.Pager.Write(pageOne); err != nil {
		return nil, err
	}

	var columnDefinitions []ColumnDefinition
	for i, c := range createStatement.Columns {
		columnDefinitions = append(columnDefinitions, ColumnDefinition{
			Name:       c.Name,
			Type:       c.Type,
			Offset:     i,
			PrimaryKey: c.PrimaryKey,
		})
	}

	tableMetadata := TableDefinition{
		Name:    createStatement.TableName,
		Columns: columnDefinitions,
	}

	return &tableMetadata, nil
}
