package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type TableMetadata struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
}

func ExecuteStatement(statement Statement) {
	switch s := statement.(type) {
	case *CreateTable:
		createTable(s)
	case *Insert:
		insert(s)
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

	metadataPath := filepath.Join("./tsql_data/", insertStatement.Table, "/metadata.json")
	dataJson, err := ioutil.ReadFile(metadataPath)

	if err != nil {
		return
	}

	var metadata TableMetadata
	err = json.Unmarshal(dataJson, &metadata)

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
