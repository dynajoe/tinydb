package main

import (
	"fmt"
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

type Operator interface {
	iOperator()
}

type BinaryOperator interface {
	iBinaryOperator()
	Operator
	Expression
}

type UnaryOperator interface {
	iUnaryOperator()
	Operator
	Expression
}

type EqualsOperator struct {
	Left  Expression
	Right Expression
}

type Expression interface {
	iExpression()
	reduce() Literal
}

type Literal struct {
	Value string
	Expression
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

func (EqualsOperator) iExpression()     {}
func (EqualsOperator) iOperator()       {}
func (EqualsOperator) iBinaryOperator() {}

func (expr EqualsOperator) reduce() Literal {
	if expr.Left.reduce().Value == expr.Right.reduce().Value {
		return Literal{
			Value: "true",
		}
	}

	return Literal{
		Value: "false",
	}
}

func (expr Literal) reduce() Literal {
	return expr
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

func parseExpression(nodify NodifyExpression) Parser {
	return oneOf([]Parser{
		parseLiteral(nodify),
		lazy(func() Parser { return parseEqualsOperator(nodify) }),
	}, nil)
}

func parseLiteral(nodify NodifyExpression) Parser {
	return requiredToken(tsqlIdentifier, func(token []item) {
		nodify(Literal{
			Value: token[0].text,
		})
	})
}

func parseBinaryOperator(nodify NodifyExpression) Parser {
	return oneOf([]Parser{
		parseEqualsOperator(nodify),
	}, nil)
}

func lazy(x func() Parser) Parser {
	return func(scanner *TSQLScanner) bool {
		return x()(scanner)
	}
}

func parseEqualsOperator(nodify NodifyExpression) Parser {
	equalsOp := EqualsOperator{}

	return all([]Parser{
		lazy(func() Parser { return parseExpression(func(left Expression) { equalsOp.Left = left }) }),
		requiredToken(tsqlWhiteSpace, nil),
		equal(),
		requiredToken(tsqlWhiteSpace, nil),
		lazy(func() Parser { return parseExpression(func(right Expression) { equalsOp.Right = right }) }),
	}, func(tokens [][]item) { nodify(equalsOp) })
}

func parseSelect(scanner *TSQLScanner) SelectStatement {
	selectStatement := &Select{}
	whereClause :=
		all([]Parser{
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlWhere, nil),
			requiredToken(tsqlWhiteSpace, nil),
			separatedBy1(
				all([]Parser{
					requiredToken(tsqlWhiteSpace, nil),
					atom(tsqlAnd),
					requiredToken(tsqlWhiteSpace, nil),
				}, nil),
				parseBinaryOperator(func(expr Expression) {
					selectStatement.Filter = expr
				}),
			),
		}, nil)

	result := scanner.run(
		all([]Parser{
			requiredToken(tsqlSelect, nil),
			requiredToken(tsqlWhiteSpace, nil),
			separatedBy1(
				all([]Parser{
					optionalToken(tsqlWhiteSpace),
					atom(tsqlComma),
					optionalToken(tsqlWhiteSpace),
				}, nil),
				all([]Parser{
					optionalToken(tsqlWhiteSpace),
					oneOf([]Parser{
						requiredToken(tsqlIdentifier, nil),
						requiredToken(tsqlAsterisk, nil),
					}, func(token []item) {
						selectStatement.Columns = append(selectStatement.Columns, token[0].text)
					}),
				}, nil),
			),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlFrom, nil),
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlIdentifier, func(token []item) {
				selectStatement.From = token[0].text
			}),
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

func equal() Parser {
	return func(scanner *TSQLScanner) bool {
		if scanner.peek().token == tsqlEquals {
			scanner.next()
			return true
		}

		return false
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
	if createStatement := parseCreateTable(scanner); createStatement != nil {
		fmt.Println("Create statement!")
		return createStatement
	}

	if insertStatement := parseInsert(scanner); insertStatement != nil {
		fmt.Println("Insert statement!")
		return insertStatement
	}

	if selectStatement := parseSelect(scanner); selectStatement != nil {
		fmt.Println("Select statement!")
		return selectStatement
	}

	return nil
}

func (scanner *TSQLScanner) peek() item {
	token := scanner.next()

	if scanner.position >= 1 {
		scanner.backup()
	}

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

type Statementify func(tokens []item) Statement

type Nodify func(tokens []item)
type NodifyMany func(tokens [][]item)
type ParseResult struct {
	statement Statement
	Error     error
}

type NodifyExpression func(expr Expression)
