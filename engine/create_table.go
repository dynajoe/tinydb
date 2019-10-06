package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func createTable(engine *Engine, createStatement *CreateTableStatement) (*TableMetadata, error) {
	tablePath := filepath.Join("./tsql_data/", strings.ToLower(createStatement.TableName))

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

	tableMetadata := TableMetadata{
		Name:    createStatement.TableName,
		Columns: createStatement.Columns,
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

	return &tableMetadata, nil
}
