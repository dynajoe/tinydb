package engine

import (
	"github.com/joeandaverde/tinydb/ast"
	"sync"

	"github.com/joeandaverde/tinydb/internal/btree"
)

// Row is a row in a result
type Row struct {
	Data    []string
	Offset  int64
	IsValid bool
}

// ColumnList represents a list of columns of a result set
type ColumnList []string

// ResultSet is the result of a query; rows are provided asynchronously
type ResultSet struct {
	Columns ColumnList
	Rows    <-chan Row
	Error   <-chan error
}

type nestedLoop struct {
	outer  executable
	inner  executable
	filter ast.Expression
}

type indexScan struct {
	index  *btree.BTree
	value  string
	table  TableDefinition
	column ColumnDefinition
}

type sequenceScan struct {
	table  TableDefinition
	filter ast.Expression
}

type executable interface {
	execute(*Engine, *ExecutionEnvironment) (*ResultSet, error)
	optimize(*Engine, *ExecutionEnvironment) executable
}

func EmptyResultSet() *ResultSet {
	rows := make(chan Row)
	close(rows)
	return &ResultSet{
		Rows: rows,
	}
}

func (s *sequenceScan) execute(engine *Engine, env *ExecutionEnvironment) (*ResultSet, error) {
	rowReader, err := newTableScanner(engine.Config, s.table.Name)

	if err != nil {
		return nil, err
	}

	results := make(chan Row)
	errorChan := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errorChan)

		for rowReader.Scan() {
			row := rowReader.Read()
			if s.filter != nil && ast.Evaluate(s.filter, evalContext{env: env, data: row.Data}).Value != true {
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

func (s *sequenceScan) optimize(engine *Engine, env *ExecutionEnvironment) executable {
	// This simply detects: customer_id = 1 or 1 = customer_id
	if op, ok := s.filter.(*ast.BinaryOperation); ok {
		ident, literal := ast.IdentLiteralOperation(op)

		if ident != nil && literal != nil && op.Operator == "=" {
			columnReference := env.ColumnLookup[ident.Value]
			if columnReference.PrimaryKey {
				return &indexScan{
					index:  engine.Indexes[s.table.Name],
					value:  literal.Value,
					table:  s.table,
					column: columnReference,
				}
			}
		}
	}

	return s
}

func (s *indexScan) execute(engine *Engine, env *ExecutionEnvironment) (*ResultSet, error) {
	rowReader, err := newTableScanner(engine.Config, s.table.Name)

	if err != nil {
		return nil, err
	}

	results := make(chan Row)
	errorChan := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errorChan)

		item := s.index.Find(&indexedField{
			value: s.value,
		})

		if f, ok := item.(*indexedField); ok {
			for rowReader.Scan() {
				row := rowReader.Read()
				for _, o := range f.offsets {
					if row.Offset == o {
						results <- row
						break
					}
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

func (s *indexScan) optimize(engine *Engine, env *ExecutionEnvironment) executable {
	return s
}

type evalContext struct {
	env  *ExecutionEnvironment
	data []string
}

type nilEvalContext struct{}

func (c nilEvalContext) GetValue(*ast.Ident) (interface{}, bool) {
	return nil, false
}

func (c evalContext) GetValue(ident *ast.Ident) (interface{}, bool) {
	if columnIndex, ok := c.env.ColumnLookup[ident.Value]; ok {
		return c.data[columnIndex.Offset], true
	}
	return nil, false
}

func (s *nestedLoop) execute(engine *Engine, env *ExecutionEnvironment) (*ResultSet, error) {
	results := make(chan Row)
	errorChan := make(chan error, 1)

	go func() {
		defer close(results)

		outerStatement, _ := s.outer.optimize(engine, env).execute(engine, env)
		innerStatement, _ := s.inner.optimize(engine, env).execute(engine, env)

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
			innerRows = append(innerRows, i.Data)
		}

		// Cartesian product
		for o := range outerStatement.Rows {
			for _, i := range innerRows {
				row := append(append([]string{}, o.Data...), i...)

				if s.filter != nil && ast.Evaluate(s.filter, &evalContext{env: env, data: row}).Value != true {
					continue
				}

				results <- Row{
					Data:    row,
					Offset:  0,
					IsValid: false,
				}
			}
		}
	}()

	return &ResultSet{
		Columns: nil,
		Rows:    results,
		Error:   errorChan,
	}, nil
}

func (s *nestedLoop) optimize(engine *Engine, env *ExecutionEnvironment) executable {
	return s
}

func buildPlan(engine *Engine, environment *ExecutionEnvironment, selectStatement *ast.SelectStatement) executable {
	if len(selectStatement.From) == 1 {
		return &sequenceScan{
			table:  environment.Tables[selectStatement.From[0].Alias],
			filter: selectStatement.Filter,
		}
	}

	return &nestedLoop{
		outer: &sequenceScan{
			table:  environment.Tables[selectStatement.From[0].Alias],
			filter: nil,
		},
		inner: &sequenceScan{
			table:  environment.Tables[selectStatement.From[1].Alias],
			filter: nil,
		},
		filter: selectStatement.Filter,
	}
}

func doSelect(engine *Engine, selectStatement *ast.SelectStatement) (*ResultSet, error) {
	environment, err := newExecutionEnvironment(engine, selectStatement.From)

	if err != nil {
		return nil, err
	}

	plan := buildPlan(engine, environment, selectStatement).optimize(engine, environment)

	return plan.execute(engine, environment)
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
