package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type TableMetadata struct {
	Name    string             `json:"name"`
	Columns []ColumnDefinition `json:"columns"`
}

type ExecutionEnvironment struct {
	ColumnLookup map[string]int
	Tables       map[string]TableMetadata
}

type SelectResult struct {
	Columns []string
	Rows    chan []string
}

func ExecuteStatement(statement Statement) {
	switch s := statement.(type) {
	case *CreateTable:
		if _, err := createTable(s); err != nil {
			fmt.Println(err)
		}
	case *Insert:
		if _, err := insert(s); err != nil {
			fmt.Println(err)
		}
	case *Select:
		startingTime := time.Now().UTC()
		i := 0

		if result, err := sqlSelect(s); err != nil {
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

func createTable(createStatement *CreateTable) (*TableMetadata, error) {
	tablePath := filepath.Join("./tsql_data/", strings.ToLower(createStatement.Name))

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
		Name:    createStatement.Name,
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

func insert(insertStatement *Insert) (rowCount int, err error) {
	fmt.Printf("inserting the heck out of %s\n", insertStatement.Table)
	emptyTables := make(map[string]string)
	var emptyColumns []string

	environment, err := getExecutionEnvironment(emptyTables)

	for k, v := range insertStatement.Values {
		fmt.Printf("inserting %s in %s\n", v, k)
	}

	metadata, err := getMetadata(insertStatement.Table)

	if err != nil {
		return 0, err
	}

	dataFile, err := os.OpenFile(filepath.Join("./tsql_data/", insertStatement.Table, "/data.csv"), os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModePerm)

	if err != nil {
		return 0, err
	}

	defer func() {
		if closeErr := dataFile.Close(); closeErr != nil {
			err = closeErr
		}
	}()

	if err != nil {
		return 0, err
	}

	writer := bufio.NewWriter(dataFile)

	defer func() {
		if flushErr := writer.Flush(); flushErr != nil {
			err = flushErr
		}
	}()

	var values []string
	for _, column := range metadata.Columns {
		values = append(values, insertStatement.Values[column.Name].reduce(emptyColumns, environment).Value)
	}

	row := strings.Join(values, ",") + "\n"

	if _, err := writer.WriteString(row); err != nil {
		return 0, err
	}

	return 1, nil
}

func sqlSelect(selectStatement *Select) (*SelectResult, error) {
	environment, err := getExecutionEnvironment(selectStatement.From)

	if err != nil {
		return nil, err
	}

	// Readers for table data
	var tables [][][]string

	// Iterating over map likely results in non-deterministic ordering.
	for _, tableInfo := range environment.Tables {
		csvFile, err := os.Open(filepath.Join("./tsql_data/", tableInfo.Name, "/data.csv"))

		if err != nil {
			return nil, err
		}

		tableCsv, _ := csv.NewReader(bufio.NewReader(csvFile)).ReadAll()

		tables = append(tables, tableCsv)
	}

	var crossProduct [][]string

	for _, records := range tables {
		if len(crossProduct) == 0 {
			crossProduct = records[:]
		} else {
			var newStuff [][]string
			for _, e := range crossProduct {
				for _, row := range records {
					newStuff = append(newStuff, append(e, row...))
				}
			}

			crossProduct = newStuff
		}
	}

	var returnedColumnIndexes []int

	for _, column := range selectStatement.Columns {
		if column == "*" {
			// Iterating over map likely results in non-deterministic ordering.
			for _, i := range environment.ColumnLookup {
				returnedColumnIndexes = append(returnedColumnIndexes, i)
			}
		} else {
			returnedColumnIndexes = append(returnedColumnIndexes, environment.ColumnLookup[column])
		}
	}

	results := make(chan []string)

	go func() {
		for _, row := range crossProduct {
			if selectStatement.Filter != nil && selectStatement.Filter.reduce(row, environment).Value != "true" {
				continue
			}

			var result []string

			for _, columnIndex := range returnedColumnIndexes {
				result = append(result, row[columnIndex])
			}

			results <- result
		}

		close(results)
	}()

	return &SelectResult{
		Rows:    results,
		Columns: nil,
	}, nil
}

func getMetadata(tablename string) (*TableMetadata, error) {
	metadataPath := filepath.Join("./tsql_data/", tablename, "/metadata.json")
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
		metadata, err := getMetadata(table)

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
