package parsing

import (
	"fmt"
	"strconv"
)

type TableMetadata struct {
	Name    string             `json:"name"`
	Columns []ColumnDefinition `json:"columns"`
}

type ExecutionEnvironment struct {
	ColumnLookup map[string]int
	Tables       map[string]TableMetadata
	Columns      []string
}

type EvaluatedExpression struct {
	Value interface{}
}

func Evaluate(expression Expression, data []string, environment *ExecutionEnvironment) EvaluatedExpression {
	return expression.Evaluate(data, environment)
}

func (o *BinaryOperation) Evaluate(data []string, environment *ExecutionEnvironment) EvaluatedExpression {
	left := o.Left.Evaluate(data, environment).Value
	right := o.Right.Evaluate(data, environment).Value

	switch o.Operator {
	case "+":
		leftIsInt := isInt(left)
		rightIsInt := isInt(right)

		if leftIsInt && rightIsInt {
			return EvaluatedExpression{
				Value: left.(int) + right.(int),
			}
		}

		panic("can only add two ints")
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

	panic("Unknown operation")
}

func (l *BasicLiteral) Evaluate(data []string, environment *ExecutionEnvironment) EvaluatedExpression {
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

	panic("unexpected token type")
}

func (l *Ident) Evaluate(data []string, environment *ExecutionEnvironment) EvaluatedExpression {
	if columnIndex, ok := environment.ColumnLookup[l.Value]; ok {
		return EvaluatedExpression{
			Value: data[columnIndex],
		}
	}

	panic(fmt.Errorf("column definition not found [%s]", l.Value))
}

func (e EvaluatedExpression) String() string {
	switch e.Value.(type) {
	case string:
		return e.Value.(string)
	case int:
		return strconv.Itoa(e.Value.(int))
	case bool:
		return strconv.FormatBool(e.Value.(bool))
	}

	panic("cant convert to string")
}

func isInt(v interface{}) bool {
	switch v.(type) {
	case int:
		return true
	}

	return false
}
