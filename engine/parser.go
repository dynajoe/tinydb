package engine

type Parser func(*TSQLScanner) (bool, interface{})

type OperatorParser func(*TSQLScanner) (bool, string)

type Nodify func(tokens []item)

type NodifyMany func(tokens [][]item)

type NodifyExpression func(expr Expression)

type NodifyOperator func(tokens []item) string

type ExpressionMaker func(op string, a Expression, b Expression) Expression

type ExpressionParser func(scanner *TSQLScanner) (bool, Expression)

type Values map[string]Expression

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

func parseCreateTable(scanner *TSQLScanner) Statement {
	createTableStatement := &CreateTableStatement{}
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
				createTableStatement.TableName = token[0].text
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

func parseInsert(scanner *TSQLScanner) *InsertStatement {
	insertTableStatement := &InsertStatement{}

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

func parseSelect(scanner *TSQLScanner) *SelectStatement {
	selectStatement := &SelectStatement{
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
		return &BinaryOperation{
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
				nodify(&Ident{
					Value: token[0].text,
				})
			}
		}),
		requiredToken(tsqlString, func(token []item) {
			if nodify != nil {
				nodify(&BasicLiteral{
					Value:     token[0].text[1 : len(token[0].text)-1],
					TokenType: token[0].token,
				})
			}
		}),
		requiredToken(tsqlNumber, func(token []item) {
			if nodify != nil {
				nodify(&BasicLiteral{
					Value:     token[0].text,
					TokenType: token[0].token,
				})
			}
		}),
		requiredToken(tsqlBoolean, func(token []item) {
			if nodify != nil {
				nodify(&BasicLiteral{
					Value:     token[0].text,
					TokenType: token[0].token,
				})
			}
		}),
	}, nil)
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
