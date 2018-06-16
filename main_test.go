package main

import "testing"

func TestParse(t *testing.T) {
	selectStatement := Parse("SELECT * FROM foo")

	if selectStatement == nil {
		t.Errorf("parsing select statement failed")
	}
}
