package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

func ExecuteStatement(statement Statement) {
	switch s := statement.(type) {
	case *CreateTable:
		createTable(s)
		break
	}
}

func createTable(createStatement *CreateTable) {
	tablePath := filepath.Join("./tsql_data/", createStatement.Name)

	os.MkdirAll(tablePath, os.ModePerm)

	f, _ := os.Create(filepath.Join(tablePath, "./metadata.json"))
	w := bufio.NewWriter(f)
	contents := fmt.Sprintf(`{ "name": "%s", "columns": [ ] }`, createStatement.Name)

	if _, err := w.WriteString(contents); err != nil {
		fmt.Println(err)
	}

	w.Flush()
}
