package engine

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

type cleanupFunc func()

func initializeTestDb() (*Engine, cleanupFunc, error) {
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
		_ = os.RemoveAll(testDir)
	}

	db := Start(NewConfig(testDir))

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

	companies := map[int]string{
		1: "Netflix",
		2: "Facebook",
		3: "Apple",
		4: "Google",
	}

	var results []int
	for companyId, companyName := range companies {
		result, err := db.Execute(fmt.Sprintf(`
			INSERT INTO company (company_id, company_name)
			VALUES (%d, '%s')
			RETURNING company_id;
		`, companyId, companyName))

		if err != nil {
			t.Error(err)
		}

		for r := range result.Rows {
			results = append(results, r.Data[0].(int))
		}
	}

	if len(results) != len(companies) {
		t.Error("unexpected number of results from insert")
	}

	for _, companyId := range results {
		statement := fmt.Sprintf(`
			SELECT companyName.company_name
			FROM company companyName
			WHERE companyName.company_id = %d AND companyName.company_name = '%s';
		`, companyId, companies[companyId])

		result, err := db.Execute(statement)
		if err != nil {
			t.Error(err)
		}

		rowCount := 0
		for range result.Rows {
			rowCount++
		}

		if rowCount != 1 {
			t.Errorf("unexpected row count [%d]", rowCount)
		}
	}
}
