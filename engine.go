package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type TableMetadata struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
}

type ExecutionEnvironment struct {
	ColumnLookup map[string]int
	Metadata     *TableMetadata
}

func ExecuteStatement(statement Statement) {
	switch s := statement.(type) {
	case *CreateTable:
		createTable(s)
	case *Insert:
		insert(s)
	case *Select:
		sqlSelect(s)
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

func sqlSelect(selectStatement *Select) {
	fmt.Printf("selecting the best rows from %s for your viewing pleasure\n", selectStatement.From)

	for _, column := range selectStatement.Columns {
		fmt.Printf("selecting %s from %s\n", column, selectStatement.From)
	}

	environment, err := getExecutionEnvironment(selectStatement.From)

	if err != nil {
		return
	}

	dataFile, err := os.Open(filepath.Join("./tsql_data/", selectStatement.From, "/data.csv"))

	if err != nil {
		return
	}

	csvReader := csv.NewReader(dataFile)

	records, err := csvReader.ReadAll()

	if err != nil {
		log.Fatal(err)
	}

	returnedColumnIndexes := []int{}
	for _, column := range selectStatement.Columns {
		if column == "*" {
			for i, _ := range environment.Metadata.Columns {
				returnedColumnIndexes = append(returnedColumnIndexes, i)
			}
		} else {

			returnedColumnIndexes = append(returnedColumnIndexes, environment.ColumnLookup[column])
		}
	}

	for _, row := range records {
		if selectStatement.Filter != nil && selectStatement.Filter.reduce().Value != "true" {
			continue
		}

		for _, columnIndex := range returnedColumnIndexes {
			fmt.Printf("%s,", row[columnIndex])
		}
		fmt.Println()
	}

}

func getMetadata(tablename string) (*TableMetadata, error) {
	metadataPath := filepath.Join("./tsql_data/", tablename, "/metadata.json")
	dataJson, err := ioutil.ReadFile(metadataPath)

	if err != nil {
		return nil, err
	}

	var metadata TableMetadata
	err = json.Unmarshal(dataJson, &metadata)

	return &metadata, err
}

func getExecutionEnvironment(tablename string) (*ExecutionEnvironment, error) {
	metadata, err := getMetadata(tablename)

	if err != nil {
		return nil, err
	}

	columnLookup := make(map[string]int)

	for i, c := range metadata.Columns {
		columnLookup[c] = i
	}

	return &ExecutionEnvironment{
		ColumnLookup: columnLookup,
		Metadata:     metadata,
	}, err
}
