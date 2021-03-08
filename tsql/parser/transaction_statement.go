package parser

import (
	"github.com/joeandaverde/tinydb/tsql/ast"
	"github.com/joeandaverde/tinydb/tsql/lexer"
	"github.com/joeandaverde/tinydb/tsql/scan"
)

func parseBegin(scanner scan.TinyScanner) (*ast.BeginStatement, error) {
	parser := allX(
		committed("BEGIN", token(lexer.TokenBegin)),
	)

	if ok, _ := parser(scanner); ok {
		return &ast.BeginStatement{}, nil
	}

	return nil, nil
}

func parseCommit(scanner scan.TinyScanner) (*ast.CommitStatement, error) {
	parser := allX(
		committed("COMMIT", keyword(lexer.TokenCommit)),
	)

	if ok, _ := parser(scanner); ok {
		return &ast.CommitStatement{}, nil
	}

	return nil, nil
}

func parseRollback(scanner scan.TinyScanner) (*ast.RollbackStatement, error) {
	parser := allX(
		committed("ROLLBACK", keyword(lexer.TokenRollback)),
	)

	if ok, _ := parser(scanner); ok {
		return &ast.RollbackStatement{}, nil
	}

	return nil, nil
}
