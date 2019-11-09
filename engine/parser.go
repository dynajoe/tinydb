package engine

type tsqlParser func(*tsqlScanner) (bool, interface{})

type tsqlOpParser func(*tsqlScanner) (bool, string)

type nodify func(tokens []item)

type nodifyMany func(tokens [][]item)

type nodifyExpression func(expr Expression)

type nodifyOperator func(tokens []item) string

type expressionMaker func(op string, a Expression, b Expression) Expression

type expressionParser func(scanner *tsqlScanner) (bool, Expression)

// Parse - parses TinySql statements
func Parse(sql string) Statement {
	scanner := &tsqlScanner{
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

func parseCreateTable(scanner *tsqlScanner) Statement {
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

func parseInsert(scanner *tsqlScanner) *InsertStatement {
	insertTableStatement := &InsertStatement{}

	var columns []string
	var values []Expression

	result, _ := scanner.start(
		all([]tsqlParser{
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
				all([]tsqlParser{
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
				all([]tsqlParser{
					optionalToken(tsqlWhiteSpace),
					makeExpressionParser(func(e Expression) {
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

func parseSelect(scanner *tsqlScanner) *SelectStatement {
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
				}, func(token []item) {
					selectStatement.Columns = append(selectStatement.Columns, token[0].text)
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
				}, func(tokens [][]item) {
					if len(tokens[1]) > 0 {
						selectStatement.From = append(selectStatement.From, TableAlias{
							name:  tokens[0][0].text,
							alias: tokens[1][1].text,
						})
					} else {
						selectStatement.From = append(selectStatement.From, TableAlias{
							name:  tokens[0][0].text,
							alias: tokens[0][0].text,
						})
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

func parseTermExpression() expressionParser {
	return func(scanner *tsqlScanner) (bool, Expression) {
		var expr Expression

		success, _ := scanner.run(
			oneOf([]tsqlParser{
				parseTerm(func(expression Expression) {
					expr = expression
				}),
				parens(lazy(func() tsqlParser {
					return func(scanner *tsqlScanner) (bool, interface{}) {
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
	return func(scanner *tsqlScanner) (bool, string) {
		var opText string

		success, _ := required(opParser, func(x []item) {
			opText = nodifyOperator(x)
		})(scanner)

		return success, opText
	}
}

func comparison() tsqlOpParser {
	return operatorParser(operator(`=`), func(tokens []item) string {
		return tokens[1].text
	})
}

func logical() tsqlOpParser {
	return operatorParser(oneOf([]tsqlParser{
		operator(`AND`),
		operator(`OR`),
	}, nil), func(tokens []item) string {
		return tokens[1].text
	})
}

func mult() tsqlOpParser {
	return operatorParser(oneOf([]tsqlParser{
		operator(`\*`),
		operator(`/`),
	}, nil), func(tokens []item) string {
		return tokens[1].text
	})
}

func sum() tsqlOpParser {
	return operatorParser(oneOf([]tsqlParser{
		operator(`\+`),
		operator(`-`),
	}, nil), func(tokens []item) string {
		return tokens[1].text
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

func optionalToken(expected Token) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
		if scanner.peek().token == expected {
			scanner.next()
		}

		return true, nil
	}
}

func requiredToken(expected Token, nodify nodify) tsqlParser {
	return required(func(scanner *tsqlScanner) (bool, interface{}) {
		if scanner.next().token == expected {
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
	return func(scanner *tsqlScanner) (bool, interface{}) {
		success, expr := parseExpression()(scanner)

		if success {
			nodify(expr)
		}

		return success, expr
	}
}

func committed(committedAt string, p tsqlParser) tsqlParser {
	return func(scanner *tsqlScanner) (bool, interface{}) {
		scanner.committed = committedAt

		if success, results := scanner.run(p); success {
			return success, results
		}

		return false, nil
	}
}
