package ast

import (
	"errors"
	"fmt"
	"strconv"
)

type EvaluatedExpression struct {
	Value interface{}
	Error error
}

func Evaluate(expression Expression, ctx EvaluationContext) EvaluatedExpression {
	return expression.Evaluate(ctx)
}

func (o *BinaryOperation) Evaluate(ctx EvaluationContext) EvaluatedExpression {
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

func (l *BasicLiteral) Evaluate(EvaluationContext) EvaluatedExpression {
	switch l.TokenType {
	case tsqlBoolean:
		value, _ := strconv.ParseBool(l.Value)
		return EvaluatedExpression{
			Value: value,
		}
	case tsqlNumber:
		value, _ := strconv.Atoi(l.Value)
		return EvaluatedExpression{
			Value: value,
		}
	case tsqlString:
		return EvaluatedExpression{
			Value: l.Value,
		}
	}

	return EvaluatedExpression{
		Error: errors.New("unexpected literal type"),
	}
}

func (i *Ident) Evaluate(ctx EvaluationContext) EvaluatedExpression {
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
