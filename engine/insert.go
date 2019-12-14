package engine

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/joeandaverde/tinydb/ast"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

func doInsert(engine *Engine, insertStatement *ast.InsertStatement) (rowCount int, returning *ResultSet, err error) {
	log.Debugf("Inserting [%d] value(s) into [%s]", len(insertStatement.Values), insertStatement.Table)

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
	for _, column := range metadata.Columns {
		value := ast.Evaluate(insertStatement.Values[column.Name], nilEvalContext{})
		values = append(values, fmt.Sprintf("%s", value))
	}

	csvWriter := csv.NewWriter(writer)

	if err := csvWriter.Write(values); err != nil {
		return 0, nil, err
	}

	return 0, EmptyResultSet(), nil
}
