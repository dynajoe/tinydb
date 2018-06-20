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
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
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
		createTable(s)
	case *Insert:
		insert(s)
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

func createTable(createStatement *CreateTable) {
	tablePath := filepath.Join("./tsql_data/", createStatement.Name)

	os.MkdirAll(tablePath, os.ModePerm)

	f, _ := os.Create(filepath.Join(tablePath, "./metadata.json"))
	w := bufio.NewWriter(f)
	defer w.Flush()

	columns := []string{}

	for _, columnDefinition := range createStatement.Columns {
		columns = append(columns, columnDefinition.Name)
	}

	tableMetadata := TableMetadata{
		Name:    createStatement.Name,
		Columns: columns,
	}

	contents, err := json.Marshal(tableMetadata)

	if err != nil {
		return
	}

	if _, err := w.Write(contents); err != nil {
		fmt.Println(err)
	}
}

func insert(insertStatement *Insert) {
	fmt.Printf("inserting the heck out of %s\n", insertStatement.Table)

	for k, v := range insertStatement.Values {
		fmt.Printf("inserting %s in %s\n", v, k)
	}

	metadata, err := getMetadata(insertStatement.Table)

	if err != nil {
		return
	}

	dataFile, err := os.OpenFile(filepath.Join("./tsql_data/", insertStatement.Table, "/data.csv"), os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer dataFile.Close()

	if err != nil {
		return
	}

	writer := bufio.NewWriter(dataFile)
	defer writer.Flush()

	values := []string{}
	for _, column := range metadata.Columns {
		values = append(values, insertStatement.Values[column])
	}

	row := strings.Join(values, ",") + "\n"

	writer.WriteString(row)
}

func sqlSelect(selectStatement *Select) (*SelectResult, error) {
	environment, err := getExecutionEnvironment(selectStatement.From)

	if err != nil {
		return nil, err
	}

	// Readers for table data
	readers := make(map[string]*csv.Reader)
	// Iterating over map likely results in non-deterministic ordering.
	for alias, tableInfo := range environment.Tables {
		dataFile, err := os.Open(filepath.Join("./tsql_data/", tableInfo.Name, "/data.csv"))

		if err != nil {
			return nil, err
		}

		defer dataFile.Close()

		tableCsv := csv.NewReader(dataFile)

		readers[alias] = tableCsv
	}

	crossProduct := [][]string{}
	// Iterating over map likely results in non-deterministic ordering.
	for _, reader := range readers {
		records, _ := reader.ReadAll()

		if len(crossProduct) == 0 {
			crossProduct = records[:]
		} else {
			newStuff := [][]string{}
			for _, e := range crossProduct {
				for _, row := range records {
					newStuff = append(newStuff, append(e, row...))
				}
			}

			crossProduct = newStuff
		}
	}

	returnedColumnIndexes := []int{}
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
			result := []string{}

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
			columnLookup[fmt.Sprintf("%s.%s", alias, c)] = i
			i++
		}

		tableMetadata[alias] = *metadata
	}

	return &ExecutionEnvironment{
		Tables:       tableMetadata,
		ColumnLookup: columnLookup,
	}, nil
}
