package engine

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

func doInsert(engine *Engine, insertStatement *InsertStatement) (rowCount int, returning *ResultSet, err error) {
	log.Debugf("Inserting [%d] value(s) into [%s]", len(insertStatement.Values), insertStatement.Table)

	var emptyTables []TableAlias
	var emptyColumns []string

	environment, err := newExecutionEnvironment(engine, emptyTables)

	if err != nil {
		return 0, nil, err
	}

	metadata, ok := engine.Tables[insertStatement.Table]

	if !ok {
		return 0, nil, fmt.Errorf("Unable to locate table %s", insertStatement.Table)
	}

	dataFilePath := filepath.Join(engine.Config.BasePath, "tsql_data", insertStatement.Table, "data.csv")
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
	for _, column := range metadata.Columns {
		value := Evaluate(insertStatement.Values[column.Name], emptyColumns, environment)
		values = append(values, fmt.Sprintf("%s", value))
	}

	csvWriter := csv.NewWriter(writer)

	if err := csvWriter.Write(values); err != nil {
		return 0, nil, err
	}

	return 0, emptyResultSet(), nil
}
