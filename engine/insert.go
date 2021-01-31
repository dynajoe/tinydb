package engine

import (
	"fmt"

	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/tsql/ast"
)

// TODO: this is to get things to compile, need to actually get auto incr key
var keys = make(map[string]int)

func nextKey(tableName string) int {
	if _, ok := keys[tableName]; !ok {
		keys[tableName] = 0
	}
	keys[tableName] = keys[tableName] + 1
	return keys[tableName]
}

func doInsert(engine *Engine, insertStatement *ast.InsertStatement) (rowCount int, returning *ResultSet, err error) {
	engine.Log.Debugf("Inserting [%d] value(s) into [%s]", len(insertStatement.Values), insertStatement.Table)

	metadata, err := engine.GetTableDefinition(insertStatement.Table)
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
	addField := func(column ColumnDefinition, value interface{}) {
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
	rowID := nextKey(insertStatement.Table)
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

	rootPage, err := engine.Pager.Read(metadata.RootPage)
	if err != nil {
		return 0, nil, err
	}

	record := storage.NewRecord(fields)
	if err := storage.WriteRecord(rootPage, rowID, record); err != nil {
		return 0, nil, err
	}
	if err := engine.Pager.Write(rootPage); err != nil {
		return 0, nil, err
	}

	return 1, &ResultSet{
		Columns: insertStatement.Returning,
		Rows:    rowsFromValues(returnValues),
		Error:   nil,
	}, nil
}

func rowsFromValues(rows ...[]interface{}) <-chan Row {
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
