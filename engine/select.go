package engine

import (
	"bufio"
	"encoding/csv"
	"os"
	"path/filepath"
)

type SelectResult struct {
	Columns []string
	Rows    chan []string
}

func doSelect(engine *Engine, selectStatement *SelectStatement) (*SelectResult, error) {
	environment, err := getExecutionEnvironment(engine, selectStatement.From)

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
			if selectStatement.Filter != nil && Evaluate(selectStatement.Filter, row, environment).Value != true {
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
