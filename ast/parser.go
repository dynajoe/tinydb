package ast

import (
	"fmt"
)

type tsqlParser func(*tinyScanner) (bool, interface{})

type tsqlOpParser func(*tinyScanner) (bool, string)

type nodify func(tokens []TinyDBItem)

type nodifyMany func(tokens [][]TinyDBItem)

type nodifyExpression func(expr Expression)

type nodifyOperator func(tokens []TinyDBItem) string

type expressionMaker func(op string, a Expression, b Expression) Expression

type expressionParser func(scanner *tinyScanner) (bool, Expression)

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

func parseCreateTable(scanner *tinyScanner) (*CreateTableStatement, error) {
	createTableStatement := &CreateTableStatement{}
	flags := make(map[string]string)

	result, _ := scanner.start(
		all([]tsqlParser{
			requiredToken(tsqlCreate, nil),
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlTable, nil),
			requiredToken(tsqlWhiteSpace, nil),
			optional(all([]tsqlParser{
				text("IF"),
				requiredToken(tsqlWhiteSpace, nil),
				text("NOT"),
				requiredToken(tsqlWhiteSpace, nil),
				text("EXISTS"),
				requiredToken(tsqlWhiteSpace, nil),
			}, nil), func(token []TinyDBItem) {
				createTableStatement.IfNotExists = true
			}),
			requiredToken(tsqlIdentifier, func(token []TinyDBItem) {
				createTableStatement.TableName = token[0].Text
			}),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlOpenParen, nil),
			optionalToken(tsqlWhiteSpace),
			separatedBy1(requiredToken(tsqlComma, nil),
				all([]tsqlParser{
					optionalToken(tsqlWhiteSpace),
					requiredToken(tsqlIdentifier, nil),
					requiredToken(tsqlWhiteSpace, nil),
					requiredToken(tsqlIdentifier, nil),
					optional(all([]tsqlParser{
						requiredToken(tsqlWhiteSpace, nil),
						text("PRIMARY"),
						requiredToken(tsqlWhiteSpace, nil),
						text("KEY"),
					}, nil), func(tokens []TinyDBItem) {
						flags["primary_key"] = "true"
					}),
					optionalToken(tsqlWhiteSpace),
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
				})),
			optionalToken(tsqlComma),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlCloseParen, nil),
			optionalToken(tsqlWhiteSpace),
		}, nil))

	if result {
		return createTableStatement, nil
	}

	return nil, nil
}

func parseInsert(scanner *tinyScanner) (*InsertStatement, error) {
	insertTableStatement := &InsertStatement{}

	var columns []string
	var values []Expression

	result, _ := scanner.start(
		all([]tsqlParser{
			requiredToken(tsqlInsert, nil),
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlInto, nil),
			requiredToken(tsqlWhiteSpace, nil),
			requiredToken(tsqlIdentifier, func(token []TinyDBItem) {
				insertTableStatement.Table = token[0].Text
			}),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlOpenParen, nil),
			separatedBy1(requiredToken(tsqlComma, nil),
				all([]tsqlParser{
					optionalToken(tsqlWhiteSpace),
					requiredToken(tsqlIdentifier, func(token []TinyDBItem) {
						columns = append(columns, token[0].Text)
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
				all([]tsqlParser{
					optionalToken(tsqlWhiteSpace),
					makeExpressionParser(func(e Expression) {
						values = append(values, e)
					}),
				}, nil),
			),
			optionalToken(tsqlWhiteSpace),
			requiredToken(tsqlCloseParen, nil),
			optional(all([]tsqlParser{
				requiredToken(tsqlWhiteSpace, nil),
				text("RETURNING"),
				requiredToken(tsqlWhiteSpace, nil),
				committed("RETURNING_COLUMNS", separatedBy1(
					commaSeparator(),
					oneOf([]tsqlParser{
						requiredToken(tsqlIdentifier, nil),
						requiredToken(tsqlAsterisk, nil),
					}, func(token []TinyDBItem) {
						insertTableStatement.Returning = append(insertTableStatement.Returning, token[0].Text)
					}),
				)),
			}, nil), nil),
		}, nil))

	if !result {
		return nil, nil
	}

	// if columns and values are not of same length or are empty blow up
	// create map
	numColumns := len(columns)
	numValues := len(values)

	if numColumns != numValues {
		return nil, nil
	}

	insertTableStatement.Values = make(map[string]Expression)

	for i := 0; i < numColumns; i++ {
		insertTableStatement.Values[columns[i]] = values[i]
	}

	return insertTableStatement, nil
}

func parseSelect(scanner *tinyScanner) (*SelectStatement, error) {
	selectStatement := &SelectStatement{}

	whereClause :=
		lazy(func() tsqlParser {
			return all([]tsqlParser{
				requiredToken(tsqlWhiteSpace, nil),
				keyword(tsqlWhere),
				committed("WHERE", makeExpressionParser(func(filter Expression) {
					selectStatement.Filter = filter
				})),
			}, nil)
		})

	success, _ := scanner.start(
		all([]tsqlParser{
			committed("SELECT", keyword(tsqlSelect)),
			committed("COLUMNS", separatedBy1(
				commaSeparator(),
				oneOf([]tsqlParser{
					requiredToken(tsqlIdentifier, nil),
					requiredToken(tsqlAsterisk, nil),
				}, func(token []TinyDBItem) {
					selectStatement.Columns = append(selectStatement.Columns, token[0].Text)
				}),
			)),
			requiredToken(tsqlWhiteSpace, nil),
			committed("FROM", keyword(tsqlFrom)),
			committed("RELATIONS", separatedBy1(
				commaSeparator(),
				all([]tsqlParser{
					committed("RELATION",
						requiredToken(tsqlIdentifier, nil)),
					optional(all([]tsqlParser{
						requiredToken(tsqlWhiteSpace, nil),
						requiredToken(tsqlIdentifier, nil),
					}, nil), nil),
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
			optional(whereClause, nil),
			optionalToken(tsqlWhiteSpace),
		}, nil),
	)

	if success {
		return selectStatement, nil
	}

	return nil, nil
}

func parseTermExpression() expressionParser {
	return func(scanner *tinyScanner) (bool, Expression) {
		var expr Expression

		success, _ := scanner.run(
			oneOf([]tsqlParser{
				parseTerm(func(expression Expression) {
					expr = expression
				}),
				parens(lazy(func() tsqlParser {
					return func(scanner *tinyScanner) (bool, interface{}) {
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

func makeBinaryExpression() expressionMaker {
	return func(operatorStr string, left Expression, right Expression) Expression {
		return &BinaryOperation{
			Left:     left,
			Right:    right,
			Operator: operatorStr,
		}
	}
}

func operatorParser(opParser tsqlParser, nodifyOperator nodifyOperator) tsqlOpParser {
	return func(scanner *tinyScanner) (bool, string) {
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
	return operatorParser(oneOf([]tsqlParser{
		operator(`AND`),
		operator(`OR`),
	}, nil), func(tokens []TinyDBItem) string {
		return tokens[1].Text
	})
}

func mult() tsqlOpParser {
	return operatorParser(oneOf([]tsqlParser{
		operator(`\*`),
		operator(`/`),
	}, nil), func(tokens []TinyDBItem) string {
		return tokens[1].Text
	})
}

func sum() tsqlOpParser {
	return operatorParser(oneOf([]tsqlParser{
		operator(`\+`),
		operator(`-`),
	}, nil), func(tokens []TinyDBItem) string {
		return tokens[1].Text
	})
}

func parseExpression() expressionParser {
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

func parseTerm(nodify nodifyExpression) tsqlParser {
	return oneOf([]tsqlParser{
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

func optionalToken(expected Token) tsqlParser {
	return func(scanner *tinyScanner) (bool, interface{}) {
		if scanner.Peek().Token == expected {
			scanner.Next()
		}

		return true, nil
	}
}

func requiredToken(expected Token, nodify nodify) tsqlParser {
	return required(func(scanner *tinyScanner) (bool, interface{}) {
		if scanner.Next().Token == expected {
			return true, nil
		}

		return false, nil
	}, nodify)
}

func operator(operatorText string) tsqlParser {
	return all([]tsqlParser{
		optionalToken(tsqlWhiteSpace),
		regex(operatorText),
		optionalToken(tsqlWhiteSpace),
	}, nil)
}

func parens(inner tsqlParser) tsqlParser {
	return all([]tsqlParser{
		requiredToken(tsqlOpenParen, nil),
		inner,
		requiredToken(tsqlCloseParen, nil),
	}, nil)
}

func commaSeparator() tsqlParser {
	return all([]tsqlParser{
		optionalToken(tsqlWhiteSpace),
		requiredToken(tsqlComma, nil),
		optionalToken(tsqlWhiteSpace),
	}, nil)
}

func keyword(token Token) tsqlParser {
	return all([]tsqlParser{
		requiredToken(token, nil),
		requiredToken(tsqlWhiteSpace, nil),
	}, nil)
}

func makeExpressionParser(nodify nodifyExpression) tsqlParser {
	return func(scanner *tinyScanner) (bool, interface{}) {
		success, expr := parseExpression()(scanner)

		if success {
			nodify(expr)
		}

		return success, expr
	}
}

func committed(committedAt string, p tsqlParser) tsqlParser {
	return func(scanner *tinyScanner) (bool, interface{}) {
		scanner.committed = committedAt

		if success, results := scanner.run(p); success {
			return success, results
		}

		return false, nil
	}
}
