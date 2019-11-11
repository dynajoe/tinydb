package engine

import (
	"io"
	"log"

	"github.com/joeandaverde/tinydb/btree"
)

// ResultSet is the result of a query; rows are provided asynchronously
type ResultSet struct {
	Columns []string
	Rows    chan []string
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

func (s *sequenceScan) execute(engine *Engine, env *ExecutionEnvironment) (*ResultSet, error) {
	csvFile, err := newTableScanner(s.table.Name)

	if err != nil {
		return nil, err
	}

	results := make(chan []string)

	go func() {
		for {
			row, err := csvFile.Read()

			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}

			if s.filter != nil && Evaluate(s.filter, row, env).Value != true {
				continue
			}

			results <- row
		}

		close(results)
	}()

	return &ResultSet{
		Rows:    results,
		Columns: nil,
	}, nil
}

func (s *indexScan) execute(engine *Engine, env *ExecutionEnvironment) (*ResultSet, error) {
	csvFile, err := newTableScanner(s.column.table)

	if err != nil {
		return nil, err
	}

	results := make(chan []string)

	go func() {
		item := s.index.Find(&indexedField{
			value: s.value,
		})

		if f, ok := item.(*indexedField); ok {
			offset := 0
			for {
				row, err := csvFile.Read()

				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}

				offset++

				if f.offset == offset {
					results <- row
				}
			}
		}

		close(results)
	}()

	return &ResultSet{
		Columns: nil,
		Rows:    results,
	}, nil
}

func (s *nestedLoop) execute(engine *Engine, env *ExecutionEnvironment) (*ResultSet, error) {
	results := make(chan []string)

	go func() {
		outerStatement, _ := optimize(s.outer, engine, env).execute(engine, env)
		innerStatement, _ := optimize(s.inner, engine, env).execute(engine, env)

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

		close(results)
	}()

	return &ResultSet{
		Columns: nil,
		Rows:    results,
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
