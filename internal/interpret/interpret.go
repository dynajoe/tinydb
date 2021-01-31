package interpret

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/joeandaverde/tinydb/engine"
	"github.com/joeandaverde/tinydb/internal/btree"
	"github.com/joeandaverde/tinydb/tsql"
	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/lexer"
)

type EvaluatedExpression struct {
	Value interface{}
	Error error
}

// EvaluationContext provides a means for resolving identifiers to values
type EvaluationContext interface {
	GetValue(ident *ast.Ident) (interface{}, bool)
}

// Execute runs a statement against the database engine
func Execute(e *engine.Engine, text string) (*ResultSet, error) {
	startingTime := time.Now().UTC()
	defer func() {
		duration := time.Now().UTC().Sub(startingTime)
		e.Log.Infof("\nDuration: %s\n", duration)
	}()

	e.Log.Debug("EXEC: ", text)

	statement, err := tsql.Parse(strings.TrimSpace(text))
	if err != nil {
		return nil, err
	}

	return executeStatement(e, statement)
}

func Evaluate(expression ast.Expression, ctx EvaluationContext) EvaluatedExpression {
	if ctx == nil {
		ctx = nilEvalContext{}
	}
	switch e := expression.(type) {
	case *ast.BinaryOperation:
		return evaluateBinaryOperation(e, ctx)
	case *ast.BasicLiteral:
		return evaluateLiteral(e, ctx)
	case *ast.Ident:
		return evaluateIdent(e, ctx)
	default:
		return EvaluatedExpression{
			Error: errors.New("unrecognized expression"),
		}
	}
}

type columnLookup struct {
	index  int
	column engine.ColumnDefinition
}

type ExecutionEnvironment struct {
	ColumnLookup map[string]columnLookup
	Tables       map[string]*engine.TableDefinition
	Columns      []string
	Indexes      map[string]*btree.BTree
	Engine       *engine.Engine
}

func newExecutionEnvironment(e *engine.Engine, tables []ast.TableAlias) (*ExecutionEnvironment, error) {
	colLookup := make(map[string]columnLookup)
	tableMetadata := make(map[string]*engine.TableDefinition)
	allMetadata := make([]*engine.TableDefinition, len(tables))
	i := 0
	for _, tableAlias := range tables {
		tableName := tableAlias.Name
		metadata, err := e.GetTableDefinition(tableName)
		if err != nil {
			return nil, fmt.Errorf("unable to locate table %s", tableName)
		}
		for _, c := range metadata.Columns {
			lookupKey := c.Name
			if tableAlias.Alias != "" {
				lookupKey = tableAlias.Alias + "." + lookupKey
			}

			colLookup[lookupKey] = columnLookup{
				index:  i,
				column: c,
			}
			i++
		}

		tableMetadata[tableAlias.Alias] = metadata
		allMetadata = append(allMetadata, metadata)
	}

	return &ExecutionEnvironment{
		Tables:       tableMetadata,
		ColumnLookup: colLookup,
		Engine:       e,
	}, nil
}

func evaluateBinaryOperation(o *ast.BinaryOperation, ctx EvaluationContext) EvaluatedExpression {
	left := Evaluate(o.Left, ctx).Value
	right := Evaluate(o.Right, ctx).Value

	switch o.Operator {
	case "+":
		leftIsInt := isInt(left)
		rightIsInt := isInt(right)

		if leftIsInt && rightIsInt {
			return EvaluatedExpression{
				Value: left.(int) + right.(int),
			}
		}

		return EvaluatedExpression{
			Error: errors.New("can only add two integers"),
		}
	case "=":
		return EvaluatedExpression{
			Value: left == right,
		}
	case "AND":
		return EvaluatedExpression{
			Value: left == true && right == true,
		}
	case "OR":
		return EvaluatedExpression{
			Value: left == true || right == true,
		}
	}

	return EvaluatedExpression{
		Error: errors.New("unknown operation"),
	}
}

func evaluateLiteral(l *ast.BasicLiteral, ctx EvaluationContext) EvaluatedExpression {
	switch l.Kind {
	case lexer.TokenBoolean:
		value, _ := strconv.ParseBool(l.Value)
		return EvaluatedExpression{
			Value: value,
		}
	case lexer.TokenNumber:
		value, _ := strconv.Atoi(l.Value)
		return EvaluatedExpression{
			Value: value,
		}
	case lexer.TokenString:
		return EvaluatedExpression{
			Value: l.Value,
		}
	}

	return EvaluatedExpression{
		Error: errors.New("unexpected literal type"),
	}
}

func evaluateIdent(i *ast.Ident, ctx EvaluationContext) EvaluatedExpression {
	if v, ok := ctx.GetValue(i); ok {
		return EvaluatedExpression{
			Value: v,
		}
	}

	return EvaluatedExpression{Error: fmt.Errorf("value [%s] not available in evaluation context", i.Value)}
}

func (e EvaluatedExpression) String() string {
	if e.Error != nil {
		return e.Error.Error()
	}

	switch e.Value.(type) {
	case string:
		return e.Value.(string)
	case int:
		return strconv.Itoa(e.Value.(int))
	case bool:
		return strconv.FormatBool(e.Value.(bool))
	}

	return fmt.Sprintf("%v", e.Value)
}

func isInt(v interface{}) bool {
	_, success := v.(int)
	return success
}

func executeStatement(engine *engine.Engine, statement ast.Statement) (*ResultSet, error) {
	switch s := (statement).(type) {
	case *ast.CreateTableStatement:
		if err := createTable(engine, s); err != nil {
			return nil, err
		}
		return EmptyResultSet(), nil
	case *ast.InsertStatement:
		_, result, err := doInsert(engine, s)
		return result, err
	case *ast.SelectStatement:
		return doSelect(engine, s)
	default:
		return nil, fmt.Errorf("unexpected statement type")
	}
}
