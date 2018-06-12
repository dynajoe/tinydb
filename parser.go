package main

type Parser struct {
	lexer     *tsqlLexer
	input     string
	items     []item
	peekCount int
}

// Statement - a TinySQL Statement
type Statement interface{}

type CreateStatement struct{}
type InsertStatement struct{}
type SelectStatement struct{}

// Parse - parses TinySql statements
func Parse(sql string) (Statement, error) {
	parser := &Parser{
		lexer:     lex("tsql", sql),
		input:     sql,
		items:     []item{},
		peekCount: 0,
	}

	return parser.parse()
}

func parseCreateStatement(lexer *tsqlLexer) (CreateStatement, error) {
	return CreateStatement{}, nil
}

func parseSelectStatement(lexer *tsqlLexer) (SelectStatement, error) {
	return SelectStatement{}, nil
}

func (parser *Parser) parse() (Statement, error) {
	return nil, nil
}

func (parser *Parser) peek() item {
	parser.peekCount++
	return parser.next()
}

func (parser *Parser) next() item {
	item := parser.lexer.nextItem()

	parser.items = append(parser.items, item)

	return item
}
