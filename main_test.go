package main

import (
	"testing"

	"github.com/joeandaverde/tinydb/engine"
)

func TestSelectStar(t *testing.T) {
	selectStatement := engine.Parse("SELECT * FROM foo")

	if selectStatement == nil {
		t.Errorf("parsing select statement failed")
	}
}

func TestSelectColumns(t *testing.T) {
	selectStatement := engine.Parse("SELECT a, b FROM foo")

	if selectStatement == nil {
		t.Errorf("parsing select statement failed")
	}
}

func TestSelectFromMultipleTables(t *testing.T) {
	selectStatement := engine.Parse("SELECT a, b FROM foo, bar")

	if selectStatement == nil {
		t.Errorf("parsing select statement failed")
	}
}

func TestSelectWhereClause(t *testing.T) {
	selectStatement := engine.Parse("SELECT a, b FROM foo, bar WHERE a = 1")

	if selectStatement == nil {
		t.Errorf("parsing select statement failed")
	}
}
