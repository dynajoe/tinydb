package engine

import (
	"fmt"

	"github.com/joeandaverde/tinydb/ast"
)

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

	for _, column := range metadata.Columns {
		v := ast.Evaluate(insertStatement.Values[column.Name], nilEvalContext{})

		if k, ok := returningLookup[column.Name]; ok {
			returnValues[k] = v
		}

		// if index, ok := engine.Indexes[metadata.Name]; ok {
		// 	f := &indexedField{value: valueString, offsets: []int64{fileOffset}}
		//
		// 	if column.PrimaryKey && index.Find(f) != nil {
		// 		return 0, EmptyResultSet(), errors.New("primary key violation")
		// 	}
		//
		// 	r := index.Insert(f)
		// 	if r != nil {
		// 		oldField := r.(*indexedField)
		// 		oldField.offsets = append(oldField.offsets, fileOffset)
		// 	}
		// }
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
