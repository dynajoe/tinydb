package main

import (
	"fmt"
	"regexp"
	"strconv"
)

type TSQLScanner struct {
	lexer    *tsqlLexer
	input    string
	items    []item
	position int
}

// Statement - a TinySQL Statement
type Statement interface {
	iStatement()
}

type DDLStatement interface {
	iDDLStatement()
	Statement
}

type InsertRows interface {
	iInsertRows()
}

type CreateStatement interface {
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

type Values map[string]string

type Select struct {
	From    string
	Columns []string
	Filter  Expression
}

type BinaryOperator interface {
	iBinaryOperator()
	Expression
}

type UnaryOperator interface {
	iUnaryOperator()
	Expression
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

func (num Number) String() string {
	return fmt.Sprintf("%d", num.Value)
}

func (lit Literal) String() string {
	return lit.Value
}

func (op BinaryOperation) String() string {
	return fmt.Sprintf("(%s %s %s)", op.Left, op.Operator, op.Right)
}

type ColumnReference struct {
	Name string
}

type ColumnType int

const (
	colTypeInt ColumnType = iota
	colTypeText
)

type ColumnDefinition struct {
	Name string
	Type string
}

type CreateTable struct {
	Name    string
	Columns []ColumnDefinition
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
			Value: string(leftNumber + rightNumber),
		}
	case "=":
		if op.Left.reduce(columns, environment).Value == op.Right.reduce(columns, environment).Value {
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

// Parse - parses TinySql statements
func Parse(sql string) Statement {
	scanner := &TSQLScanner{
		lexer:    lex("tsql", sql),
		input:    sql,
		items:    []item{},
		position: 0,
	}

	fmt.Println(sql)

	return scanner.parse()
}

func parseCreateTable(scanner *TSQLScanner) DDLStatement {
	createTableStatement := &CreateTable{}

	result := scanner.run(
		all([]Parser{
			requiredToken(tsqlCreate, nil),
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlTable, nil),
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlIdentifier, func(token []item) {
				createTableStatement.Name = token[0].text
			}),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlOpenParen, nil),
			optionalToken(tsqlWhiteSpace),
			separatedBy1(atom(tsqlComma),
				all([]Parser{
					optionalToken(tsqlWhiteSpace),
					requiredToken(tsqlIdentifier, nil),
					requiredToken(tsqlWhiteSpace, nil),
					requiredToken(tsqlIdentifier, nil),
					optionalToken(tsqlWhiteSpace),
				}, func(tokens [][]item) {
					columnName := tokens[1][0].text
					columnType := tokens[3][0].text

					createTableStatement.Columns = append(createTableStatement.Columns, ColumnDefinition{
						Name: columnName,
						Type: columnType,
					})
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

	columns := []string{}
	values := []string{}

	result := scanner.run(
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
			separatedBy1(atom(tsqlComma),
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
			separatedBy1(atom(tsqlComma),
				all([]Parser{
					optionalToken(tsqlWhiteSpace),
					requiredToken(tsqlIdentifier, func(token []item) {
						values = append(values, token[0].text)
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

	insertTableStatement.Values = make(map[string]string)

	for i := 0; i < numColumns; i++ {
		insertTableStatement.Values[columns[i]] = values[i]
	}

	return insertTableStatement
}

func parseTermExpression() ExpressionParser {
	return func(scanner *TSQLScanner) (bool, Expression) {
		var expr Expression

		success := scanner.run(
			oneOf([]Parser{
				parseTerm(func(expression Expression) {
					expr = expression
				}),
				parens(lazy(func() Parser {
					return func(scanner *TSQLScanner) bool {
						s, e := parseExpression()(scanner)
						expr = e
						return s
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

		success := required(opParser, func(x []item) {
			opText = nodifyOperator(x)
		})(scanner)

		return success, opText
	}
}

func comparison() OperatorParser {
	return operatorParser(oneOf([]Parser{
		operator(`=`),
	}, nil), func(tokens []item) string {
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
		return tokens[0].text
	})
}

func sum() OperatorParser {
	return operatorParser(oneOf([]Parser{
		operator(`\+`),
		operator(`-`),
	}, nil), func(tokens []item) string {
		return tokens[0].text
	})
}

func parseExpression() ExpressionParser {
	return chainl(
		chainl(
			parseTermExpression(),
			makeBinaryExpression(),
			mult(),
		),
		makeBinaryExpression(),
		sum(),
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
					Value: token[0].text,
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

func chainl(expressionParser ExpressionParser, f ExpressionMaker, opParser OperatorParser) ExpressionParser {
	return func(scanner *TSQLScanner) (bool, Expression) {
		success, expression := expressionParser(scanner)

		if success {
			for {
				if success, op := opParser(scanner); success {
					if success, right := expressionParser(scanner); success {
						expression = f(op, expression, right)
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
	return func(scanner *TSQLScanner) bool {
		return x()(scanner)
	}
}

func regex(r string) Parser {
	return func(scanner *TSQLScanner) bool {
		next := scanner.peek()

		if regexp.MustCompile(r).MatchString(next.text) {
			scanner.next()
			return true
		}

		return false
	}
}

func operator(operatorText string) Parser {
	return all([]Parser{
		optionalToken(tsqlWhiteSpace),
		regex(operatorText),
		optionalToken(tsqlWhiteSpace),
	}, nil)
}

func equal() Parser {
	return all([]Parser{
		optionalToken(tsqlWhiteSpace),
		atom(tsqlEquals),
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
		atom(tsqlComma),
		optionalToken(tsqlWhiteSpace),
	}, nil)
}

func keywordSeparator(token Token) Parser {
	return all([]Parser{
		requiredToken(tsqlWhiteSpace, nil),
		atom(token),
		requiredToken(tsqlWhiteSpace, nil),
	}, nil)
}

func keyword(token Token) Parser {
	return all([]Parser{
		requiredToken(token, nil),
		requiredToken(tsqlWhiteSpace, nil),
	}, nil)
}

func parseWhereClause(nodify NodifyExpression) Parser {

	return func(scanner *TSQLScanner) bool {
		success, expr := chainl(
			chainl(
				parseExpression(),
				makeBinaryExpression(),
				comparison(),
			),
			makeBinaryExpression(),
			logical(),
		)(scanner)

		if success {
			nodify(expr)
		}

		return success
	}
}

func parseSelect(scanner *TSQLScanner) SelectStatement {
	selectStatement := &Select{}

	whereClause :=
		lazy(func() Parser {
			return all([]Parser{
				requiredToken(tsqlWhiteSpace, nil),
				keyword(tsqlWhere),
				parseWhereClause(func(filter Expression) {
					selectStatement.Filter = filter
				}),
			}, nil)
		})

	result := scanner.run(
		all([]Parser{
			keyword(tsqlSelect),
			separatedBy1(
				commaSeparator(),
				oneOf([]Parser{
					requiredToken(tsqlIdentifier, nil),
					requiredToken(tsqlAsterisk, nil),
				}, func(token []item) {
					selectStatement.Columns = append(selectStatement.Columns, token[0].text)
				}),
			),
			requiredToken(tsqlWhiteSpace, nil),
			keyword(tsqlFrom),
			separatedBy1(
				commaSeparator(),
				requiredToken(tsqlIdentifier, func(token []item) {
					selectStatement.From = token[0].text
				}),
			),
			optional(whereClause, nil),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlEOF, nil),
		}, nil),
	)

	if result {
		return selectStatement
	}

	return nil
}

func atom(token Token) Parser {
	return func(scanner *TSQLScanner) bool {
		return token == scanner.next().token
	}
}

func optionalToken(expected Token) Parser {
	return func(scanner *TSQLScanner) bool {
		if scanner.peek().token == expected {
			scanner.next()
		}

		return true
	}
}

func requiredToken(expected Token, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) bool {
		if scanner.peek().token == expected {
			token := scanner.next()

			if nodify != nil {
				nodify([]item{token})
			}

			return true
		}

		return false
	}
}

func (scanner *TSQLScanner) parse() Statement {
	success := scanner.run(parseWhereClause(func(expr Expression) {
		fmt.Println(expr)
	}))

	if success {
		fmt.Println("Parse success")
	} else {
		fmt.Println("Parse Fail")
	}

	// if createStatement := parseCreateTable(scanner); createStatement != nil {
	// 	fmt.Println("Create statement!")
	// 	return createStatement
	// }

	// if insertStatement := parseInsert(scanner); insertStatement != nil {
	// 	fmt.Println("Insert statement!")
	// 	return insertStatement
	// }

	// if selectStatement := parseSelect(scanner); selectStatement != nil {
	// 	fmt.Println("Select statement!")
	// 	return selectStatement
	// }

	return nil
}

func (scanner *TSQLScanner) peek() item {
	token := scanner.next()

	if scanner.position >= 1 {
		scanner.backup()
	}

	fmt.Printf("Peek: \"%s\"\n", token.text)

	return token
}

func (scanner *TSQLScanner) backup() {
	if scanner.position > 0 {
		scanner.position--
	} else {
		panic("Attempting to back up before any tokens")
	}
}

func separatedBy1(separator Parser, parser Parser) Parser {
	return func(scanner *TSQLScanner) bool {
		if parser(scanner) {
			for {
				if separator(scanner) {
					if !parser(scanner) {
						return false
					}
				} else {
					return true
				}
			}
		}

		return false
	}
}

func oneOrMore(parser Parser) Parser {
	return func(scanner *TSQLScanner) bool {
		if !parser(scanner) {
			return false
		}

		return zeroOrMore(parser)(scanner)
	}
}

func zeroOrMore(parser Parser) Parser {
	return func(scanner *TSQLScanner) bool {
		for {
			if !parser(scanner) {
				break
			}
		}

		return true
	}
}

func all(parsers []Parser, nodify NodifyMany) Parser {
	return func(scanner *TSQLScanner) bool {
		start := scanner.position
		matchesAll := true
		tokens := [][]item{}

		for _, parser := range parsers {
			before := scanner.position

			if !parser(scanner) {
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

		return matchesAll
	}
}

func oneOf(parsers []Parser, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) bool {
		start := scanner.position

		for _, parser := range parsers {
			if parser(scanner) {
				token := scanner.items[start:scanner.position]

				if nodify != nil {
					nodify(token)
				}

				return true
			}

			scanner.position = start
		}

		return false
	}
}

func optional(parser Parser, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) bool {
		start := scanner.position

		if parser(scanner) {
			token := scanner.items[start:scanner.position]

			if nodify != nil {
				nodify(token)
			}

			return true
		}

		scanner.position = start
		return true
	}
}

func required(parser Parser, nodify Nodify) Parser {
	return func(scanner *TSQLScanner) bool {
		start := scanner.position

		if parser(scanner) {
			token := scanner.items[start:scanner.position]

			if nodify != nil {
				nodify(token)
			}

			return true
		}

		scanner.position = start
		return false
	}
}
func (scanner *TSQLScanner) run(parser Parser) bool {
	start := scanner.position

	if !parser(scanner) {
		scanner.position = start
		return false
	}

	return true
}

func (scanner *TSQLScanner) next() (token item) {
	if scanner.position >= len(scanner.items) {
		token = scanner.lexer.nextItem()
		scanner.items = append(scanner.items, token)
	} else {
		token = scanner.items[scanner.position]
	}

	scanner.position++

	return token
}

func (scanner *TSQLScanner) nextNonSpace() (token item) {
	for {
		token = scanner.next()

		if token.token != tsqlWhiteSpace {
			break
		}
	}

	return token
}

func (scanner *TSQLScanner) skipWhiteSpace() {
	for scanner.peek().token == tsqlWhiteSpace {
		scanner.next()
	}
}

type Node struct {
	Name  string
	Value string
}

type Parser func(*TSQLScanner) bool
type OperatorParser func(*TSQLScanner) (bool, string)
type Statementify func(tokens []item) Statement

type Nodify func(tokens []item)
type NodifyMany func(tokens [][]item)
type ParseResult struct {
	statement Statement
	Error     error
}

type NodifyExpression func(expr Expression)
type NodifyExpression2 func(expr1 []item, expr2 []item)

type NodifyOperator func(tokens []item) string
type ExpressionMaker func(op string, a Expression, b Expression) Expression
type ExpressionParser func(scanner *TSQLScanner) (bool, Expression)
