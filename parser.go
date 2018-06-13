package main

import (
	"fmt"
)

type Parser struct {
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

// Parse - parses TinySql statements
func Parse(sql string) Statement {
	parser := &Parser{
		lexer:    lex("tsql", sql),
		input:    sql,
		items:    []item{},
		position: 0,
	}

	return parser.parse()
}

func parseCreateTable(parser *Parser) DDLStatement {
	createTableStatement := &CreateTable{}

	result := parser.run(
		all([]ItemPredicate{
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
				all([]ItemPredicate{
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

func parseInsert(parser *Parser) InsertStatement {
	insertTableStatement := &Insert{}

	columns := []string{}
	values := []string{}

	result := parser.run(
		all([]ItemPredicate{
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
				all([]ItemPredicate{
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
				all([]ItemPredicate{
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

func parseSelect(parser *Parser) SelectStatement {

}

func atom(token Token) ItemPredicate {
	return func(parser *Parser) bool {
		return token == parser.next().token
	}
}

func optionalToken(expected Token) ItemPredicate {
	return func(parser *Parser) bool {
		if parser.peek().token == expected {
			parser.next()
		}

		return true
	}
}

func requiredToken(expected Token, nodify Nodify) ItemPredicate {
	return func(parser *Parser) bool {
		if parser.peek().token == expected {
			token := parser.next()

			if nodify != nil {
				nodify([]item{token})
			}

			return true
		}

		return false
	}
}

func (parser *Parser) parse() Statement {
	if createStatement := parseCreateTable(parser); createStatement != nil {
		fmt.Println("Create statement!")
		return createStatement
	}

	if insertStatement := parseInsert(parser); insertStatement != nil {
		fmt.Println("Insert statement!")
		return insertStatement
	}

	return nil
}

func (parser *Parser) peek() item {
	token := parser.next()

	fmt.Printf("peek \"%s\"", token)

	if parser.position >= 1 {
		parser.backup()
	}

	return token
}

func (parser *Parser) backup() {
	if parser.position > 0 {
		parser.position--
	} else {
		panic("Attempting to back up before any tokens")
	}
}

func separatedBy1(separator ItemPredicate, predicate ItemPredicate) ItemPredicate {
	return func(parser *Parser) bool {
		if predicate(parser) {
			return separator(parser) && predicate(parser)
		}

		return false
	}
}

func oneOrMore(predicate ItemPredicate) ItemPredicate {
	return func(parser *Parser) bool {
		if !predicate(parser) {
			return false
		}

		return zeroOrMore(predicate)(parser)
	}
}

func zeroOrMore(predicate ItemPredicate) ItemPredicate {
	return func(parser *Parser) bool {
		for {
			if !predicate(parser) {
				break
			}
		}

		return true
	}
}

func all(predicates []ItemPredicate, nodify NodifyMany) ItemPredicate {
	return func(parser *Parser) bool {
		start := parser.position
		matchesAll := true
		tokens := [][]item{}

		for i := 0; i < len(predicates); i++ {
			before := parser.position

			if !predicates[i](parser) {
				matchesAll = false
				break
			}

			tokens = append(tokens, parser.items[before:parser.position])
		}

		if !matchesAll {
			parser.position = start
		} else if nodify != nil {
			nodify(tokens)
		}

		return matchesAll
	}
}

func (parser *Parser) run(predicate ItemPredicate) bool {
	start := parser.position

	if !predicate(parser) {
		parser.position = start
		return false
	}

	return true
}

func (parser *Parser) next() (token item) {
	if parser.position >= len(parser.items) {
		token = parser.lexer.nextItem()
		parser.items = append(parser.items, token)
	} else {
		token = parser.items[parser.position]
	}

	parser.position++
	fmt.Println(token)

	return token
}

func (parser *Parser) nextNonSpace() (token item) {
	for {
		token = parser.next()

		if token.token != tsqlWhiteSpace {
			break
		}
	}

	return token
}

func (parser *Parser) skipWhiteSpace() {
	for parser.peek().token == tsqlWhiteSpace {
		parser.next()
	}
}

type Node struct {
	Name  string
	Value string
}

type ItemPredicate func(*Parser) bool

type Statementify func(tokens []item) Statement

type Nodify func(tokens []item)
type NodifyMany func(tokens [][]item)
type ParseResult struct {
	statement Statement
	Error     error
}
