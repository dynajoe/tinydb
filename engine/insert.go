package engine

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
)

func doInsert(engine *Engine, insertStatement *InsertStatement) (rowCount int, err error) {
	fmt.Printf("inserting the heck out of %s\n", insertStatement.Table)

	var emptyTables []TableAlias
	var emptyColumns []string

	environment, err := getExecutionEnvironment(engine, emptyTables)

	for k, v := range insertStatement.Values {
		fmt.Printf("inserting %s in %s\n", v, k)
	}

	metadata := engine.Tables[insertStatement.Table]

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
		value := Evaluate(insertStatement.Values[column.Name], emptyColumns, environment)
		values = append(values, fmt.Sprintf("%s", value))
	}

	csvWriter := csv.NewWriter(writer)

	if err := csvWriter.Write(values); err != nil {
		return 0, err
	}

	return 1, nil
}
