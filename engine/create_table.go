package engine

import (
	"github.com/joeandaverde/tinydb/ast"
	"github.com/joeandaverde/tinydb/internal/storage"
)

func createTable(engine *Engine, createStatement *ast.CreateTableStatement) (*TableDefinition, error) {
	var columnDefinitions []ColumnDefinition
	for i, c := range createStatement.Columns {
		columnDefinitions = append(columnDefinitions, ColumnDefinition{
			Name:       c.Name,
			Type:       storage.SQLTypeFromString(c.Type),
			Offset:     i,
			PrimaryKey: c.PrimaryKey,
		})
	}

	tableMetadata := &TableDefinition{
		Name:    createStatement.TableName,
		Columns: columnDefinitions,
	}

	if engine.useVirtualMachine {
		instructions := CreateTableInstructions(createStatement)
		program := NewProgram(engine, instructions)
		if err := program.Run(); err != nil {
			return nil, err
		}
		return tableMetadata, nil
	}

	pageOne, err := engine.Pager.Read(1)
	if err != nil {
		return nil, err
	}
	// TODO: the recordKey should be an int from an auto index perhaps?
	recordKey := nextKey("master")

	// Allocate a page for the new table
	rootPage, err := engine.Pager.Allocate()
	if err != nil {
		return nil, err
	}

	// Update Page 1 with the new table record
	tableRecord := storage.NewMasterTableRecord("table", createStatement.TableName,
		createStatement.TableName, rootPage.PageNumber, createStatement.RawText)

	if err := storage.WriteRecord(pageOne, recordKey, tableRecord); err != nil {
		return nil, err
	}
	if err := engine.Pager.Write(pageOne, rootPage); err != nil {
		return nil, err
	}

	return tableMetadata, nil
}
