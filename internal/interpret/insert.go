package interpret

import (
	"fmt"

	"github.com/joeandaverde/tinydb/engine"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/tsql/ast"
)

func doInsert(e *engine.Engine, insertStatement *ast.InsertStatement) (rowCount int, returning *ResultSet, err error) {
	e.Log.Debugf("Inserting [%d] value(s) into [%s]", len(insertStatement.Values), insertStatement.Table)

	metadata, err := e.GetTableDefinition(insertStatement.Table)
	if err != nil {
		return 0, nil, fmt.Errorf("unable to locate table %s", insertStatement.Table)
	}

	returningLookup := make(map[string]int)
	returnValues := make([]interface{}, len(insertStatement.Returning))

	if len(insertStatement.Returning) > 0 {
		for i, c := range insertStatement.Returning {
			returningLookup[c] = i
		}
	}

	var fields []*storage.Field
	addField := func(column engine.ColumnDefinition, value interface{}) {
		if k, ok := returningLookup[column.Name]; ok {
			returnValues[k] = value
		}
		switch value.(type) {
		case string:
			if column.Type != storage.Text {
				panic("type conversion not implemented")
			}
		case int:
			if column.Type != storage.Integer {
				panic("type conversion not implemented")
			}
		case byte:
			if column.Type != storage.Byte {
				panic("type conversion not implemented")
			}
		}
		fields = append(fields, &storage.Field{
			Type: column.Type,
			Data: value,
		})
	}

	for _, column := range metadata.Columns {
		expr, ok := insertStatement.Values[column.Name]
		if !ok {
			addField(column, column.DefaultValue())
			continue
		}

		// TODO: this value type may need to be cast or asserted
		v := Evaluate(expr, nilEvalContext{})
		addField(column, v.Value)
	}

	if err := e.InsertRecord(insertStatement.Table, fields); err != nil {
		return 0, nil, err
	}

	return 1, &ResultSet{
		Columns: insertStatement.Returning,
		Rows:    rowsFromValues(returnValues),
		Error:   nil,
	}, nil
}

func rowsFromValues(rows ...[]interface{}) <-chan engine.Row {
	resultChan := make(chan engine.Row, len(rows))

	go func() {
		defer close(resultChan)

		for _, r := range rows {
			resultChan <- engine.Row{
				Data:    r,
				Offset:  0,
				IsValid: false,
			}
		}
	}()

	return resultChan
}
