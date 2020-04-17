package engine

import (
	"fmt"

	"github.com/joeandaverde/tinydb/ast"
	"github.com/joeandaverde/tinydb/internal/storage"
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

	fields := []*storage.Field{}
	var primaryKey int
	for _, column := range metadata.Columns {
		expr, ok := insertStatement.Values[column.Name]
		if !ok {
			fields = append(fields, &storage.Field{
				Type: column.Type,
				Data: column.DefaultValue(),
			})
			continue
		}

		v := ast.Evaluate(expr, nilEvalContext{})

		switch c := v.Value.(type) {
		case string:
			fields = append(fields, &storage.Field{
				Type: storage.Text,
				Data: c,
			})
		case int, int16, int32, int64, uint, uint16, uint32, uint64:
			// TODO: technically need to handle signed values differently
			fields = append(fields, &storage.Field{
				Type: storage.Integer,
				Data: c,
			})
		case byte, int8:
			fields = append(fields, &storage.Field{
				Type: storage.Byte,
				Data: c,
			})
		}
	}

	rootPage, err := engine.Pager.Read(metadata.RootPage)
	if err != nil {
		return 0, nil, err
	}

	if primaryKey == 0 {
		primaryKey = nextKey(insertStatement.Table)
	}
	record := storage.NewRecord(primaryKey, fields)
	if err := storage.WriteRecord(rootPage, record); err != nil {
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

		for range rows {
			resultChan <- Row{
				Data:    nil,
				Offset:  0,
				IsValid: false,
			}
		}
	}()

	return resultChan
}
