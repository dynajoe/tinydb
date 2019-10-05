package engine

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	. "github.com/joeandaverde/tinydb/parsing"
)

func Run(text string) {
	fmt.Printf("Input:\n%s;\n\n", text)

	result := Parse(strings.TrimSpace(text))
	fmt.Printf("Statement:\n%s;\n\n", result)

	if result != nil {
		ExecuteStatement(result)
	}
}

func ExecuteStatement(statement Statement) {
	switch s := (statement).(type) {
	case *CreateTableStatement:
		if _, err := createTable(s); err != nil {
			fmt.Println(err)
		}
	case *InsertStatement:
		if _, err := doInsert(s); err != nil {
			fmt.Println(err)
		}
	case *SelectStatement:
		startingTime := time.Now().UTC()
		i := 0

		if result, err := doSelect(s); err != nil {
			fmt.Println(err)
		} else {
			for row := range result.Rows {
				fmt.Println(row)
				i++
			}
		}

		duration := time.Now().UTC().Sub(startingTime)

		fmt.Printf("\n%d rows (%s)\n", i, duration)
	}
}

func getTableMetadata(tableName string) (*TableMetadata, error) {
	metadataPath := filepath.Join("./tsql_data/", tableName, "/metadata.json")
	data, err := ioutil.ReadFile(metadataPath)

	if err != nil {
		return nil, err
	}

	var metadata TableMetadata
	err = json.Unmarshal(data, &metadata)

	return &metadata, err
}

func getExecutionEnvironment(tables map[string]string) (*ExecutionEnvironment, error) {
	columnLookup := make(map[string]int)
	tableMetadata := make(map[string]TableMetadata)

	i := 0
	for alias, table := range tables {
		metadata, err := getTableMetadata(table)

		if err != nil {
			return nil, err
		}

		for _, c := range metadata.Columns {
			columnLookup[fmt.Sprintf("%s.%s", alias, c.Name)] = i
			i++
		}

		tableMetadata[alias] = *metadata
	}

	return &ExecutionEnvironment{
		Tables:       tableMetadata,
		ColumnLookup: columnLookup,
	}, nil
}
