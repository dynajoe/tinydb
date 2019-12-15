package main

import (
	"fmt"
	"github.com/joeandaverde/tinydb/ast"
	"io/ioutil"
	"os"
	"testing"

	"github.com/joeandaverde/tinydb/engine"
)

func TestSelectStar(t *testing.T) {
	selectStatement, _ := ast.Parse("SELECT * FROM foo")

	if selectStatement == nil {
		t.Errorf("parsing select statement failed")
	}
}

func TestSelectColumns(t *testing.T) {
	selectStatement, _ := ast.Parse("SELECT a, b FROM foo")

	if selectStatement == nil {
		t.Errorf("parsing select statement failed")
	}
}

func TestSelectFromMultipleTables(t *testing.T) {
	selectStatement, _ := ast.Parse("SELECT a, b FROM foo, bar")

	if selectStatement == nil {
		t.Errorf("parsing select statement failed")
	}
}

func TestSelectWhereClause(t *testing.T) {
	selectStatement, _ := ast.Parse("SELECT a, b FROM foo, bar WHERE a = 1")

	if selectStatement == nil {
		t.Errorf("parsing select statement failed")
	}
}

type cleanupFunc func()

func initializeTestDb() (*engine.Engine, cleanupFunc, error) {
	createTableStatement := `
	CREATE TABLE IF NOT EXISTS company (
		company_id int PRIMARY KEY,
		company_name text
	);`

	var (
		testDir string
		err     error
	)

	if testDir, err = ioutil.TempDir(os.TempDir(), "tinydb"); err != nil {
		return nil, nil, err
	}

	cleanUp := func() {
		os.RemoveAll(testDir)
	}

	db := engine.Start(testDir)

	if _, err := db.Execute(createTableStatement); err != nil {
		return nil, cleanUp, err
	}

	return db, cleanUp, nil
}

func TestInsert(t *testing.T) {
	db, cleanUp, err := initializeTestDb()
	if cleanUp != nil {
		defer cleanUp()
	}
	if err != nil {
		t.Fatal(err.Error())
	}

	companies := []string{"Netflix", "Facebook", "Apple", "Google"}
	var results []string

	for i, c := range companies {
		result, err := db.Execute(fmt.Sprintf(`
			INSERT INTO company (company_id, company_name)
			VALUES ('%d', '%s')
			RETURNING company_id;
		`, i, c))

		if err != nil {
			t.Error(err)
		}

		for r := range result.Rows {
			results = append(results, r.Data[0])
		}
	}

	statement := `
		SELECT c.company_name
		FROM company c
		WHERE c.company_name = 'Google';
	`

	result, err := db.Execute(statement)

	if err != nil {
		t.Error(err)
	}

	rowCount := 0
	for r := range result.Rows {
		fmt.Println(r)
		rowCount++
	}

	fmt.Printf("%d row(s)\n", rowCount)
}
