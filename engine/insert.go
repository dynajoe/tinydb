package engine

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/joeandaverde/tinydb/ast"
	"os"
	"path/filepath"
)

func doInsert(engine *Engine, insertStatement *ast.InsertStatement) (rowCount int, returning *ResultSet, err error) {
	engine.Log.Debugf("Inserting [%d] value(s) into [%s]", len(insertStatement.Values), insertStatement.Table)

	metadata, ok := engine.Tables[insertStatement.Table]

	if !ok {
		return 0, nil, fmt.Errorf("unable to locate table %s", insertStatement.Table)
	}

	dataFilePath := filepath.Join(engine.Config.DataDir, insertStatement.Table, "data.csv")
	dataFile, err := os.OpenFile(dataFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModePerm)

	if err != nil {
		return 0, nil, err
	}

	defer func() {
		if closeErr := dataFile.Close(); closeErr != nil {
			err = closeErr
		}
	}()

	if err != nil {
		return 0, nil, err
	}

	writer := bufio.NewWriter(dataFile)

	defer func() {
		if flushErr := writer.Flush(); flushErr != nil {
			err = flushErr
		}
	}()

	var values []string

	fileInfo, _ := dataFile.Stat()
	fileOffset := fileInfo.Size()
	values = append(values, fmt.Sprintf("%d", fileOffset))

	returningLookup := make(map[string]int)
	returnValues := make([]string, len(insertStatement.Returning))

	if len(insertStatement.Returning) > 0 {
		for i, c := range insertStatement.Returning {
			returningLookup[c] = i
		}
	}

	for _, column := range metadata.Columns {
		value := ast.Evaluate(insertStatement.Values[column.Name], nilEvalContext{})
		valueString := fmt.Sprintf("%s", value)
		values = append(values, valueString)

		if k, ok := returningLookup[column.Name]; ok {
			returnValues[k] = valueString
		}

		if index, ok := engine.Indexes[metadata.Name]; ok {
			f := &indexedField{value: valueString, offsets: []int64{fileOffset}}

			if column.PrimaryKey && index.Find(f) != nil {
				return 0, EmptyResultSet(), errors.New("primary key violation")
			}

			r := index.Insert(f)
			if r != nil {
				oldField := r.(*indexedField)
				oldField.offsets = append(oldField.offsets, fileOffset)
			}
		}
	}

	csvWriter := csv.NewWriter(writer)

	if err := csvWriter.Write(values); err != nil {
		return 0, nil, err
	}

	return 1, &ResultSet{
		Columns: insertStatement.Returning,
		Rows:    rowsFromValues(returnValues),
		Error:   nil,
	}, nil
}

func rowsFromValues(rows ...[]string) <-chan Row {
	resultChan := make(chan Row, len(rows))

	go func() {
		defer close(resultChan)

		for _, r := range rows {
			resultChan <- Row{
				Data:    r,
				Offset:  0,
				IsValid: false,
			}
		}
	}()

	return resultChan
}
