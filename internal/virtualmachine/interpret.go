package virtualmachine

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/joeandaverde/tinydb/internal/metadata"
	"github.com/joeandaverde/tinydb/internal/storage"
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

type nilEvalContext struct{}

func (nilEvalContext) GetValue(ident *ast.Ident) (interface{}, bool) {
	return nil, false
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
	column *metadata.ColumnDefinition
}

type ExecutionEnvironment struct {
	ColumnLookup map[string]columnLookup
	Tables       map[string]*metadata.TableDefinition
	Columns      []string
	Pager        storage.Pager
}

func newExecutionEnvironment(pager storage.Pager, tables []ast.TableAlias) (*ExecutionEnvironment, error) {
	colLookup := make(map[string]columnLookup)
	tableMetadata := make(map[string]*metadata.TableDefinition)
	allMetadata := make([]*metadata.TableDefinition, len(tables))
	i := 0
	for _, tableAlias := range tables {
		tableName := tableAlias.Name
		metadata, err := metadata.GetTableDefinition(pager, tableName)
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
		Pager:        pager,
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
