package engine

import (
	"io"
	"log"
	"sync"

	"github.com/joeandaverde/tinydb/btree"
)

// ResultSet is the result of a query; rows are provided asynchronously
type ResultSet struct {
	Columns []string
	Rows    <-chan []string
	Error   <-chan error
}

type nestedLoop struct {
	outer  planItem
	inner  planItem
	filter Expression
}

type indexScan struct {
	index  *btree.BTree
	value  string
	column *ColumnReference
}

type sequenceScan struct {
	table  *TableDefinition
	filter Expression
}

type planItem interface {
	execute(*Engine, *ExecutionEnvironment) (*ResultSet, error)
}

func emptyResultSet() *ResultSet {
	rows := make(chan []string)
	var columns []string
	close(rows)

	return &ResultSet{
		Columns: columns,
		Rows:    rows,
	}
}

func (s *sequenceScan) execute(engine *Engine, env *ExecutionEnvironment) (*ResultSet, error) {
	csvFile, err := newTableScanner(engine.Config, s.table.Name)

	if err != nil {
		return nil, err
	}

	results := make(chan []string)
	errorChan := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errorChan)

		for {
			row, err := csvFile.Read()

			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
				errorChan <- err
				break
			}

			if s.filter != nil && Evaluate(s.filter, row, env).Value != true {
				continue
			}

			results <- row
		}
	}()

	return &ResultSet{
		Rows:    results,
		Error:   errorChan,
		Columns: nil,
	}, nil
}

func (s *indexScan) execute(engine *Engine, env *ExecutionEnvironment) (*ResultSet, error) {
	csvFile, err := newTableScanner(engine.Config, s.column.table)

	if err != nil {
		return nil, err
	}

	results := make(chan []string)
	errorChan := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errorChan)

		item := s.index.Find(&indexedField{
			value: s.value,
		})

		if f, ok := item.(*indexedField); ok {
			offset := 0
			for {
				row, err := csvFile.Read()

				if err != nil {
					if err != io.EOF {
						log.Fatal(err)
						errorChan <- err
					}

					break
				}

				offset++

				if f.offset == offset {
					results <- row
				}
			}
		}

	}()

	return &ResultSet{
		Columns: nil,
		Error:   errorChan,
		Rows:    results,
	}, nil
}

func mergeErrors(chans ...<-chan error) <-chan error {
	out := make(chan error, len(chans))
	var wg sync.WaitGroup

	wg.Add(len(chans))
	for _, c := range chans {
		go func(c <-chan error) {
			defer wg.Done()
			for e := range c {
				out <- e
			}
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (s *nestedLoop) execute(engine *Engine, env *ExecutionEnvironment) (*ResultSet, error) {
	results := make(chan []string)
	errorChan := make(chan error, 1)

	go func() {
		defer close(results)

		outerStatement, _ := optimize(s.outer, engine, env).execute(engine, env)
		innerStatement, _ := optimize(s.inner, engine, env).execute(engine, env)

		innerErrors := mergeErrors(outerStatement.Error, innerStatement.Error)

		go func() {
			defer close(errorChan)

			for e := range innerErrors {
				errorChan <- e
			}
		}()

		// Materialize the inner dataset, ideally filter
		innerRows := make([][]string, 0)
		for i := range innerStatement.Rows {
			innerRows = append(innerRows, i)
		}

		// Cartesian product
		for o := range outerStatement.Rows {
			for _, i := range innerRows {
				row := append(append([]string{}, o...), i...)

				if s.filter != nil && Evaluate(s.filter, row, env).Value != true {
					continue
				}

				results <- row
			}
		}
	}()

	return &ResultSet{
		Columns: nil,
		Rows:    results,
		Error:   errorChan,
	}, nil
}

func identLiteralOperation(op *BinaryOperation) (*Ident, *BasicLiteral) {
	if leftIdent, rightLiteral := asIdent(op.Left), asLiteral(op.Right); leftIdent != nil && rightLiteral != nil {
		return leftIdent, rightLiteral
	}

	if rightIdent, leftLiteral := asIdent(op.Right), asLiteral(op.Left); rightIdent != nil && leftLiteral != nil {
		return rightIdent, leftLiteral
	}

	return nil, nil
}

func optimize(plan planItem, engine *Engine, environment *ExecutionEnvironment) planItem {
	if s, ok := plan.(*sequenceScan); ok {
		// This simply detects: customer_id = 1 or 1 = customer_id
		if op, ok := s.filter.(*BinaryOperation); ok {
			ident, literal := identLiteralOperation(op)

			if ident != nil && literal != nil && op.Operator == "=" {
				columnReference := environment.ColumnLookup[ident.Value]
				if columnReference.definition.PrimaryKey {
					return &indexScan{
						index:  engine.Indexes[columnReference.table],
						value:  literal.Value,
						column: columnReference,
					}
				}
			}
		}
	}

	return plan
}

func asIdent(e Expression) *Ident {
	if op, ok := e.(*Ident); ok {
		return op
	}

	return nil
}

func asLiteral(e Expression) *BasicLiteral {
	if op, ok := e.(*BasicLiteral); ok {
		return op
	}

	return nil
}

func buildPlan(engine *Engine, environment *ExecutionEnvironment, selectStatement *SelectStatement) planItem {
	if len(selectStatement.From) == 1 {
		return &sequenceScan{
			table:  environment.Tables[selectStatement.From[0].alias],
			filter: selectStatement.Filter,
		}
	}

	return &nestedLoop{
		outer: &sequenceScan{
			table:  environment.Tables[selectStatement.From[0].alias],
			filter: nil,
		},
		inner: &sequenceScan{
			table:  environment.Tables[selectStatement.From[1].alias],
			filter: nil,
		},
		filter: selectStatement.Filter,
	}
}

func doSelect(engine *Engine, selectStatement *SelectStatement) (*ResultSet, error) {
	environment, err := newExecutionEnvironment(engine, selectStatement.From)

	if err != nil {
		return nil, err
	}

	plan := optimize(buildPlan(engine, environment, selectStatement), engine, environment)

	return plan.execute(engine, environment)
}
