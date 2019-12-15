package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/joeandaverde/tinydb/ast"
	"os"
	"path/filepath"
	"strings"
)

func createTable(engine *Engine, createStatement *ast.CreateTableStatement) (*TableDefinition, error) {
	tablePath := filepath.Join(engine.Config.DataDir, strings.ToLower(createStatement.TableName))

	if _, err := os.Stat(tablePath); !createStatement.IfNotExists && !os.IsNotExist(err) {
		return nil, fmt.Errorf("table already exists")
	}

	// The table doesn't exist, proceed.
	if err := os.MkdirAll(tablePath, os.ModePerm); err != nil {
		return nil, err
	}

	f, err := os.Create(filepath.Join(tablePath, "./metadata.json"))

	if err != nil {
		return nil, err
	}

	w := bufio.NewWriter(f)

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

	contents, err := json.Marshal(tableMetadata)

	if err != nil {
		return nil, err
	}

	if _, err := w.Write(contents); err != nil {
		return nil, err
	}

	if err := w.Flush(); err != nil {
		return nil, err
	}

	_, err = os.Create(filepath.Join(tablePath, "./data.csv"))

	return &tableMetadata, nil
}
