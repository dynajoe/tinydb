package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Parser func(*TSQLScanner) (bool, interface{})
type OperatorParser func(*TSQLScanner) (bool, string)

type Nodify func(tokens []item)
type NodifyMany func(tokens [][]item)

type NodifyExpression func(expr Expression)
type NodifyOperator func(tokens []item) string
type ExpressionMaker func(op string, a Expression, b Expression) Expression
type ExpressionParser func(scanner *TSQLScanner) (bool, Expression)

// Statement - a TinySQL Statement
type Statement interface {
	iStatement()
}

type DDLStatement interface {
	iDDLStatement()
	Statement
}

type InsertStatement interface {
	iInsertStatement()
	Statement
}

type SelectStatement interface {
	iSelectStatement()
	Statement
}

type Values map[string]Expression

type Select struct {
	From    map[string]string
	Columns []string
	Filter  Expression
}

type Expression interface {
	iExpression()
	reduce([]string, *ExecutionEnvironment) Literal
}

type Literal struct {
	Value string
	Expression
}

type Number struct {
	Value int
	Expression
}

type BinaryOperation struct {
	Left     Expression
	Right    Expression
	Operator string
	Expression
}

func (stmt Select) String() string {
	return fmt.Sprintf("SELECT %s\nFROM %s\nWHERE %s", stmt.Columns, stmt.From, stmt.Filter)
}

func (num Number) String() string {
	return fmt.Sprintf("int: %d", num.Value)
}

func (lit Literal) String() string {
	return lit.Value
}

func (op BinaryOperation) String() string {
	return fmt.Sprintf("(%s %s %s)", op.Left, op.Operator, op.Right)
}

type ColumnDefinition struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"is_primary_key"`
}

type CreateTable struct {
	Name        string
	IfNotExists bool
	Columns     []ColumnDefinition
}

type Insert struct {
	Table  string
	Values Values
}

func (*Select) iStatement()       {}
func (*Select) iSelectStatement() {}

func (*Insert) iStatement()       {}
func (*Insert) iInsertStatement() {}

func (*CreateTable) iStatement()       {}
func (*CreateTable) iCreateStatement() {}
func (*CreateTable) iDDLStatement()    {}

func (Literal) iExpression() {}

func (BinaryOperation) iExpression()     {}
func (BinaryOperation) iBinaryOperator() {}

func (op BinaryOperation) reduce(columns []string, environment *ExecutionEnvironment) Literal {
	switch op.Operator {
	case "+":
		leftNumber, _ := strconv.Atoi(op.Left.reduce(columns, environment).Value)
		rightNumber, _ := strconv.Atoi(op.Right.reduce(columns, environment).Value)

		return Literal{
			Value: strconv.FormatInt(int64(leftNumber)+int64(rightNumber), 10),
		}
	case "=":
		left := op.Left.reduce(columns, environment).Value
		right := op.Right.reduce(columns, environment).Value

		if left == right {
			return Literal{
				Value: "true",
			}
		}

		return Literal{
			Value: "false",
		}
	case "AND":
		if op.Left.reduce(columns, environment).Value == "true" && op.Right.reduce(columns, environment).Value == "true" {
			return Literal{
				Value: "true",
			}
		}

		return Literal{
			Value: "false",
		}
	case "OR":
		if op.Left.reduce(columns, environment).Value == "true" || op.Right.reduce(columns, environment).Value == "true" {
			return Literal{
				Value: "true",
			}
		}

		return Literal{
			Value: "false",
		}
	}

	panic("Unknown operation")
}

func (lit Literal) reduce(columns []string, environment *ExecutionEnvironment) Literal {
	if columnIndex, ok := environment.ColumnLookup[lit.Value]; ok {
		return Literal{
			Value: columns[columnIndex],
		}
	}

	return lit
}

func (num Number) reduce(columns []string, environment *ExecutionEnvironment) Literal {
	return Literal{
		Value: string(num.Value),
	}
}

// Parse - parses TinySql statements
func Parse(sql string) Statement {
	scanner := &TSQLScanner{
		lexer:    lex("tsql", sql),
		input:    sql,
		items:    []item{},
		position: 0,
	}

	if createStatement := parseCreateTable(scanner); createStatement != nil {
		return createStatement
	}

	if insertStatement := parseInsert(scanner); insertStatement != nil {
		return insertStatement
	}

	if selectStatement := parseSelect(scanner); selectStatement != nil {
		return selectStatement
	}

	return nil
}

func parseCreateTable(scanner *TSQLScanner) DDLStatement {
	createTableStatement := &CreateTable{}
	flags := make(map[string]string)

	result, _ := scanner.start(
		all([]Parser{
			requiredToken(tsqlCreate, nil),
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlTable, nil),
			requiredToken(tsqlWhiteSpace, nil),
			optional(all([]Parser{
				text("IF"),
				requiredToken(tsqlWhiteSpace, nil),
				text("NOT"),
				requiredToken(tsqlWhiteSpace, nil),
				text("EXISTS"),
				requiredToken(tsqlWhiteSpace, nil),
			}, nil), func(token []item) {
				createTableStatement.IfNotExists = true
			}),
			requiredToken(tsqlIdentifier, func(token []item) {
				createTableStatement.Name = token[0].text
			}),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlOpenParen, nil),
			optionalToken(tsqlWhiteSpace),
			separatedBy1(requiredToken(tsqlComma, nil),
				all([]Parser{
					optionalToken(tsqlWhiteSpace),
					requiredToken(tsqlIdentifier, nil),
					requiredToken(tsqlWhiteSpace, nil),
					requiredToken(tsqlIdentifier, nil),
					optional(all([]Parser{
						requiredToken(tsqlWhiteSpace, nil),
						text("PRIMARY"),
						requiredToken(tsqlWhiteSpace, nil),
						text("KEY"),
					}, nil), func(tokens []item) {
						flags["primary_key"] = "true"
					}),
					optionalToken(tsqlWhiteSpace),
				}, func(tokens [][]item) {
					columnName := tokens[1][0].text
					columnType := tokens[3][0].text

					_, isPrimaryKey := flags["primary_key"]

					createTableStatement.Columns = append(createTableStatement.Columns, ColumnDefinition{
						Name:       columnName,
						Type:       columnType,
						PrimaryKey: isPrimaryKey,
					})

					flags = make(map[string]string)
				})),
			optionalToken(tsqlComma),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlCloseParen, nil),
			optionalToken(tsqlWhiteSpace),
		}, nil))

	if result {
		return createTableStatement
	}

	return nil
}

func parseInsert(scanner *TSQLScanner) InsertStatement {
	insertTableStatement := &Insert{}

	var columns []string
	var values []Expression

	result, _ := scanner.start(
		all([]Parser{
			requiredToken(tsqlInsert, nil),
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlInto, nil),
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlIdentifier, func(token []item) {
				insertTableStatement.Table = token[0].text
			}),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlOpenParen, nil),
			separatedBy1(requiredToken(tsqlComma, nil),
				all([]Parser{
					optionalToken(tsqlWhiteSpace),
					requiredToken(tsqlIdentifier, func(token []item) {
						columns = append(columns, token[0].text)
					}),
				}, nil),
			),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlCloseParen, nil),

			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlValues, nil),
			requiredToken(tsqlWhiteSpace, nil),

			requiredToken(tsqlOpenParen, nil),
			separatedBy1(requiredToken(tsqlComma, nil),
				all([]Parser{
					optionalToken(tsqlWhiteSpace),
					expressionParser(func(e Expression) {
						values = append(values, e)
					}),
				}, nil),
			),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlCloseParen, nil),
		}, nil))

	if !result {
		return nil
	}

	// if columns and values are not of same length or are empty blow up
	// create map
	numColumns := len(columns)
	numValues := len(values)

	if numColumns != numValues {
		return nil
	}

	insertTableStatement.Values = make(map[string]Expression)

	for i := 0; i < numColumns; i++ {
		insertTableStatement.Values[columns[i]] = values[i]
	}

	return insertTableStatement
}

func parseTermExpression() ExpressionParser {
	return func(scanner *TSQLScanner) (bool, Expression) {
		var expr Expression

		success, _ := scanner.run(
			oneOf([]Parser{
				parseTerm(func(expression Expression) {
					expr = expression
				}),
				parens(lazy(func() Parser {
					return func(scanner *TSQLScanner) (bool, interface{}) {
						s, e := parseExpression()(scanner)

						if s {
							expr = e
							return s, e
						}

						return false, s
					}
				})),
			}, nil))

		return success, expr
	}
}

func makeBinaryExpression() ExpressionMaker {
	return func(operatorStr string, left Expression, right Expression) Expression {
		return BinaryOperation{
			Left:     left,
			Right:    right,
			Operator: operatorStr,
		}
	}
}

func operatorParser(opParser Parser, nodifyOperator NodifyOperator) OperatorParser {
	return func(scanner *TSQLScanner) (bool, string) {
		var opText string

		success, _ := required(opParser, func(x []item) {
			opText = nodifyOperator(x)
		})(scanner)

		return success, opText
	}
}

func comparison() OperatorParser {
	return operatorParser(operator(`=`), func(tokens []item) string {
		return tokens[1].text
	})
}

func logical() OperatorParser {
	return operatorParser(oneOf([]Parser{
		operator(`AND`),
		operator(`OR`),
	}, nil), func(tokens []item) string {
		return tokens[1].text
	})
}

func mult() OperatorParser {
	return operatorParser(oneOf([]Parser{
		operator(`\*`),
		operator(`/`),
	}, nil), func(tokens []item) string {
		return tokens[1].text
	})
}

func sum() OperatorParser {
	return operatorParser(oneOf([]Parser{
		operator(`\+`),
		operator(`-`),
	}, nil), func(tokens []item) string {
		return tokens[1].text
	})
}

func parseExpression() ExpressionParser {
	return chainl(
		chainl(
			chainl(
				chainl(
					parseTermExpression(),
					makeBinaryExpression(),
					mult(),
				),
				makeBinaryExpression(),
				sum(),
			),
			makeBinaryExpression(),
			comparison(),
		),
		makeBinaryExpression(),
		logical(),
	)
}

func parseTerm(nodify NodifyExpression) Parser {
	return oneOf([]Parser{
		requiredToken(tsqlIdentifier, func(token []item) {
			if nodify != nil {
				nodify(Literal{
					Value: token[0].text,
				})
			}
		}),
		requiredToken(tsqlString, func(token []item) {
			if nodify != nil {
				nodify(Literal{
					Value: token[0].text[1 : len(token[0].text)-1],
				})
			}
		}),
		requiredToken(tsqlNumber, func(token []item) {
			num, _ := strconv.Atoi(token[0].text)

			if nodify != nil {
				nodify(Number{
					Value: num,
				})
			}
		}),
	}, nil)
}

func chainl(expressionParser ExpressionParser, expressionMaker ExpressionMaker, opParser OperatorParser) ExpressionParser {
	return func(scanner *TSQLScanner) (bool, Expression) {
		success, expression := expressionParser(scanner)

		if success {
			for {
				if os, op := opParser(scanner); os {
					if ps, right := expressionParser(scanner); ps {
						expression = expressionMaker(op, expression, right)
					} else {
						return false, nil
					}
				} else {
					return true, expression
				}
			}
		}

		return false, expression
	}
}

func lazy(x func() Parser) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		return x()(scanner)
	}
}

func text(r string) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		next := scanner.peek()

		if strings.ToLower(r) == strings.ToLower(next.text) {
			scanner.next()
			return true, r
		}

		return false, nil
	}
}

func regex(r string) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		next := scanner.peek()

		if regexp.MustCompile(r).MatchString(next.text) {
			scanner.next()
			return true, r
		}

		return false, nil
	}
}

func operator(operatorText string) Parser {
	return all([]Parser{
		optionalToken(tsqlWhiteSpace),
		regex(operatorText),
		optionalToken(tsqlWhiteSpace),
	}, nil)
}

func parens(inner Parser) Parser {
	return all([]Parser{
		requiredToken(tsqlOpenParen, nil),
		inner,
		requiredToken(tsqlCloseParen, nil),
	}, nil)
}

func commaSeparator() Parser {
	return all([]Parser{
		optionalToken(tsqlWhiteSpace),
		requiredToken(tsqlComma, nil),
		optionalToken(tsqlWhiteSpace),
	}, nil)
}

func keyword(token Token) Parser {
	return all([]Parser{
		requiredToken(token, nil),
		requiredToken(tsqlWhiteSpace, nil),
	}, nil)
}

func expressionParser(nodify NodifyExpression) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		success, expr := parseExpression()(scanner)

		if success {
			nodify(expr)
		}

		return success, expr
	}
}

func committed(committedAt string, p Parser) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		scanner.committed = committedAt

		if success, results := scanner.run(p); success {
			return success, results
		}

		return false, nil
	}
}

func parseSelect(scanner *TSQLScanner) SelectStatement {
	selectStatement := &Select{
		From: make(map[string]string),
	}

	whereClause :=
		lazy(func() Parser {
			return all([]Parser{
				requiredToken(tsqlWhiteSpace, nil),
				keyword(tsqlWhere),
				committed("WHERE", expressionParser(func(filter Expression) {
					selectStatement.Filter = filter
				})),
			}, nil)
		})

	success, _ := scanner.start(
		all([]Parser{
			committed("SELECT", keyword(tsqlSelect)),
			committed("COLUMNS", separatedBy1(
				commaSeparator(),
				oneOf([]Parser{
					requiredToken(tsqlIdentifier, nil),
					requiredToken(tsqlAsterisk, nil),
				}, func(token []item) {
					selectStatement.Columns = append(selectStatement.Columns, token[0].text)
				}),
			)),
			requiredToken(tsqlWhiteSpace, nil),
			committed("FROM", keyword(tsqlFrom)),
			committed("RELATIONS", separatedBy1(
				commaSeparator(),
				all([]Parser{
					committed("RELATION", requiredToken(tsqlIdentifier, nil)),
					optional(all([]Parser{
						requiredToken(tsqlWhiteSpace, nil),
						requiredToken(tsqlIdentifier, nil),
					}, nil), nil),
				}, func(tokens [][]item) {
					if len(tokens[1]) > 0 {
						selectStatement.From[tokens[1][1].text] = tokens[0][0].text
					} else {
						selectStatement.From[tokens[0][0].text] = tokens[0][0].text
					}
				}),
			)),
			optional(whereClause, nil),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlEOF, nil),
		}, nil),
	)

	if success {
		return selectStatement
	}

	return nil
}

func optionalToken(expected Token) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		if scanner.peek().token == expected {
			scanner.next()
		}

		return true, nil
	}
}

func requiredToken(expected Token, nodify Nodify) Parser {
	return required(func(scanner *TSQLScanner) (bool, interface{}) {
		if scanner.next().token == expected {
			return true, nil
		}

		return false, nil
	}, nodify)
}

func separatedBy1(separator Parser, parser Parser) Parser {
	return all([]Parser{
		parser,
		zeroOrMore(all([]Parser{
			separator,
			parser,
		}, nil)),
	}, nil)
}

func zeroOrMore(parser Parser) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		var results []interface{}

		for {
			if success, result := parser(scanner); success {
				results = append(results, result)
			} else {
				break
			}
		}

		return true, results
	}
}

func all(parsers []Parser, nodify NodifyMany) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		start := scanner.position
		matchesAll := true
		var tokens [][]item

		for _, parser := range parsers {
			before := scanner.position

			if success, _ := parser(scanner); !success {
				matchesAll = false
				break
			}

			tokens = append(tokens, scanner.items[before:scanner.position])
		}

		if !matchesAll {
			scanner.position = start
		} else if nodify != nil {
			nodify(tokens)
		}

		return matchesAll, tokens
	}
}

func oneOf(parsers []Parser, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		start := scanner.position

		for _, parser := range parsers {
			if success, result := parser(scanner); success {
				token := scanner.items[start:scanner.position]

				if nodify != nil {
					nodify(token)
				}

				return true, result
			}

			scanner.position = start
		}

		return false, nil
	}
}

func optional(parser Parser, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		start := scanner.position

		if success, _ := parser(scanner); success {
			token := scanner.items[start:scanner.position]

			if nodify != nil {
				nodify(token)
			}

			return true, token
		}

		scanner.position = start
		return true, nil
	}
}

func required(parser Parser, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) (bool, interface{}) {
		start := scanner.position

		if success, result := parser(scanner); success {
			token := scanner.items[start:scanner.position]

			if nodify != nil {
				nodify(token)
			}

			return true, result
		}

		scanner.position = start
		return false, nil
	}
}
