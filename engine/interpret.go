package engine

import (
	"errors"
	"fmt"
	"strconv"

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

func Evaluate(expression ast.Expression, ctx EvaluationContext) EvaluatedExpression {
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
