package ast

import (
	"fmt"
)

type Parser func(TinyScanner) (bool, interface{})

type ExpressionParser func(TinyScanner) (bool, Expression)

type tsqlOpParser func(TinyScanner) (bool, string)

type nodify func(tokens []TinyDBItem)

type nodifyMany func(tokens [][]TinyDBItem)

type nodifyExpression func(expr Expression)

type nodifyOperator func(tokens []TinyDBItem) string

type expressionMaker func(op string, a Expression, b Expression) Expression

// Parse - parses TinySql statements
func Parse(sql string) (Statement, error) {
	scanner := &tinyScanner{
		lexer:    NewLexer(sql),
		input:    sql,
		items:    []TinyDBItem{},
		position: 0,
	}

	makeParseError := func(statementType string, err error) error {
		if err != nil {
			return fmt.Errorf("failed parsing [%s] at [%s]", statementType, scanner.committed)
		}

		return nil
	}

	if createStatement, err := parseCreateTable(scanner); createStatement != nil || err != nil {
		return createStatement, makeParseError("CREATE", err)
	}

	if insertStatement, err := parseInsert(scanner); insertStatement != nil || err != nil {
		return insertStatement, makeParseError("INSERT", err)
	}

	if selectStatement, err := parseSelect(scanner); selectStatement != nil || err != nil {
		return selectStatement, makeParseError("SELECT", err)
	}

	return nil, nil
}

func parseCreateTable(scanner TinyScanner) (*CreateTableStatement, error) {
	createTableStatement := CreateTableStatement{}
	flags := make(map[string]string)

	scanner.Reset()

	columnDefinition := all([]Parser{
		optWS,
		requiredToken(tsqlIdentifier, nil),
		reqWS,
		requiredToken(tsqlIdentifier, nil),
		optional(all([]Parser{
			reqWS,
			text("PRIMARY"),
			reqWS,
			text("KEY"),
		}, nil), func(tokens []TinyDBItem) {
			flags["primary_key"] = "true"
		}),
		optWS,
	}, func(tokens [][]TinyDBItem) {
		columnName := tokens[1][0].Text
		columnType := tokens[3][0].Text

		_, isPrimaryKey := flags["primary_key"]

		createTableStatement.Columns = append(createTableStatement.Columns, ColumnDefinition{
			Name:       columnName,
			Type:       columnType,
			PrimaryKey: isPrimaryKey,
		})

		flags = make(map[string]string)
	})

	ok, _ := allX(
		keyword(tsqlCreate),
		keyword(tsqlTable),
		optional(
			allX(keyword(tsqlIf), keyword(tsqlNot), keyword(tsqlExists)),
			func(token []TinyDBItem) {
				createTableStatement.IfNotExists = true
			}),
		ident(func(tableName string) {
			createTableStatement.TableName = tableName
		}),
		parensCommaSep(columnDefinition),
	)(scanner)

	if ok {
		return &createTableStatement, nil
	}

	return nil, nil
}

func parseInsert(scanner TinyScanner) (*InsertStatement, error) {
	insertTableStatement := InsertStatement{}

	var columns []string
	var values []Expression

	returningClause := allX(
		keyword(tsqlReturning),
		committed("RETURNING_COLUMNS", commaSeparated(
			oneOf([]Parser{
				token(tsqlIdentifier),
				token(tsqlAsterisk),
			}, func(token []TinyDBItem) {
				insertTableStatement.Returning = append(insertTableStatement.Returning, token[0].Text)
			}),
		)),
	)

	scanner.Reset()
	ok, _ := allX(
		keyword(tsqlInsert),
		keyword(tsqlInto),
		ident(func(tableName string) {
			insertTableStatement.Table = tableName
		}),
		parensCommaSep(
			ident(func(column string) {
				columns = append(columns, column)
			}),
		),
		keyword(tsqlValues),
		parensCommaSep(
			makeExpressionParser(func(e Expression) {
				values = append(values, e)
			}),
		),
		optionalX(returningClause),
	)(scanner)

	if !ok {
		return nil, nil
	}

	// if columns and values are not of same length or are empty blow up
	// create map
	numColumns := len(columns)
	numValues := len(values)

	if numColumns != numValues {
		return nil, fmt.Errorf("unexpected number of values")
	}

	insertTableStatement.Values = make(map[string]Expression)

	for i := 0; i < numColumns; i++ {
		insertTableStatement.Values[columns[i]] = values[i]
	}

	return &insertTableStatement, nil
}

func parseSelect(scanner TinyScanner) (*SelectStatement, error) {
	selectStatement := SelectStatement{}

	whereClause := allX(
		keyword(tsqlWhere),
		committed("WHERE", makeExpressionParser(func(filter Expression) {
			selectStatement.Filter = filter
		})),
	)

	scanner.Reset()
	ok, _ := allX(
		committed("SELECT", keyword(tsqlSelect)),
		committed("COLUMNS", commaSeparated(
			oneOf([]Parser{
				token(tsqlIdentifier),
				token(tsqlAsterisk),
			}, func(token []TinyDBItem) {
				selectStatement.Columns = append(selectStatement.Columns, token[0].Text)
			}),
		)),
		committed("FROM", keyword(tsqlFrom)),
		committed("RELATIONS", commaSeparated(
			all([]Parser{
				committed("RELATION", token(tsqlIdentifier)),
				optionalX(allX(
					reqWS,
					token(tsqlIdentifier),
				)),
			}, func(tokens [][]TinyDBItem) {
				if len(tokens[1]) > 0 {
					selectStatement.From = append(selectStatement.From, TableAlias{
						Name:  tokens[0][0].Text,
						Alias: tokens[1][1].Text,
					})
				} else {
					selectStatement.From = append(selectStatement.From, TableAlias{
						Name:  tokens[0][0].Text,
						Alias: tokens[0][0].Text,
					})
				}
			}),
		)),
		optionalX(whereClause),
	)(scanner)

	if ok {
		return &selectStatement, nil
	}

	return nil, nil
}

func parseTermExpression() ExpressionParser {
	return func(scanner TinyScanner) (bool, Expression) {
		var expr Expression

		scanner.Reset()
		ok, _ := oneOf([]Parser{
			parseTerm(func(expression Expression) {
				expr = expression
			}),
			parens(lazy(func() Parser {
				return func(scanner TinyScanner) (bool, interface{}) {
					s, e := parseExpression()(scanner)

					if s {
						expr = e
						return s, e
					}

					return false, s
				}
			})),
		}, nil)(scanner)

		return ok, expr
	}
}

func makeBinaryExpression() expressionMaker {
	return func(operatorStr string, left Expression, right Expression) Expression {
		return &BinaryOperation{
			Left:     left,
			Right:    right,
			Operator: operatorStr,
		}
	}
}

func operatorParser(opParser Parser, nodifyOperator nodifyOperator) tsqlOpParser {
	return func(scanner TinyScanner) (bool, string) {
		var opText string

		success, _ := required(opParser, func(x []TinyDBItem) {
			opText = nodifyOperator(x)
		})(scanner)

		return success, opText
	}
}

func comparison() tsqlOpParser {
	return operatorParser(operator(`=`), func(tokens []TinyDBItem) string {
		return tokens[1].Text
	})
}

func logical() tsqlOpParser {
	return operatorParser(oneOf([]Parser{
		operator(`AND`),
		operator(`OR`),
	}, nil), func(tokens []TinyDBItem) string {
		return tokens[1].Text
	})
}

func mult() tsqlOpParser {
	return operatorParser(oneOf([]Parser{
		operator(`\*`),
		operator(`/`),
	}, nil), func(tokens []TinyDBItem) string {
		return tokens[1].Text
	})
}

func sum() tsqlOpParser {
	return operatorParser(oneOf([]Parser{
		operator(`\+`),
		operator(`-`),
	}, nil), func(tokens []TinyDBItem) string {
		return tokens[1].Text
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

func parseTerm(nodify nodifyExpression) Parser {
	return oneOf([]Parser{
		requiredToken(tsqlIdentifier, func(token []TinyDBItem) {
			if nodify != nil {
				nodify(&Ident{
					Value: token[0].Text,
				})
			}
		}),
		requiredToken(tsqlString, func(token []TinyDBItem) {
			if nodify != nil {
				nodify(&BasicLiteral{
					Value:     token[0].Text[1 : len(token[0].Text)-1],
					TokenType: token[0].Token,
				})
			}
		}),
		requiredToken(tsqlNumber, func(token []TinyDBItem) {
			if nodify != nil {
				nodify(&BasicLiteral{
					Value:     token[0].Text,
					TokenType: token[0].Token,
				})
			}
		}),
		requiredToken(tsqlBoolean, func(token []TinyDBItem) {
			if nodify != nil {
				nodify(&BasicLiteral{
					Value:     token[0].Text,
					TokenType: token[0].Token,
				})
			}
		}),
	}, nil)
}

var optWS = optionalToken(tsqlWhiteSpace)
var reqWS = requiredToken(tsqlWhiteSpace, nil)
var eofParser = requiredToken(tsqlEOF, nil)

func optionalToken(expected Token) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		if scanner.Peek().Token == expected {
			scanner.Next()
		}

		return true, nil
	}
}

func ident(n func(string)) Parser {
	return requiredToken(tsqlIdentifier, func(tokens []TinyDBItem) {
		n(tokens[0].Text)
	})
}

func token(expected Token) Parser {
	return requiredToken(expected, nil)
}

func requiredToken(expected Token, nodify nodify) Parser {
	return required(func(scanner TinyScanner) (bool, interface{}) {
		if scanner.Next().Token == expected {
			return true, nil
		}

		return false, nil
	}, nodify)
}

func operator(operatorText string) Parser {
	return all([]Parser{
		optWS,
		regex(operatorText),
		optWS,
	}, nil)
}

func parens(inner Parser) Parser {
	return allX(
		optWS,
		requiredToken(tsqlOpenParen, nil),
		optWS,
		inner,
		optWS,
		requiredToken(tsqlCloseParen, nil),
		optWS,
	)
}

func parensCommaSep(p Parser) Parser {
	return parens(commaSeparated(p))
}

func commaSeparated(p Parser) Parser {
	return allX(
		optWS,
		separatedBy1(commaSeparator, p),
		optWS,
	)
}

var commaSeparator = allX(
	optWS,
	token(tsqlComma),
	optWS,
)

func keyword(t Token) Parser {
	return allX(
		optWS,
		token(t),
		oneOf([]Parser{eofParser, optWS}, nil), // Should this be required white space?
	)
}

func makeExpressionParser(nodify nodifyExpression) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		success, expr := parseExpression()(scanner)

		if success {
			nodify(expr)
		}

		return success, expr
	}
}

func committed(committedAt string, p Parser) Parser {
	return func(scanner TinyScanner) (bool, interface{}) {
		scanner.Commit(committedAt)
		_, reset := scanner.Mark()

		if success, results := p(scanner); success {
			return success, results
		}

		reset()
		return false, nil
	}
}
