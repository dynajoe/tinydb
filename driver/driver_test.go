package driver

import (
	"database/sql"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDriver_Open(t *testing.T) {
	assert := require.New(t)
	tempDir, err := ioutil.TempDir(os.TempDir(), "tinydb")
	assert.NoError(err)

	db, err := sql.Open("tinydb", tempDir)
	assert.NoError(err)
	assert.NotNil(db)
}

func TestDriver_Exec(t *testing.T) {
	assert := require.New(t)
	tempDir, err := ioutil.TempDir(os.TempDir(), "tinydb")
	assert.NoError(err)

	db, err := sql.Open("tinydb", tempDir)
	assert.NoError(err)
	assert.NotNil(db)

	res, err := db.Exec("CREATE TABLE foo (name text);")
	assert.NoError(err)
	assert.NotNil(res)

	res, err = db.Exec("INSERT INTO foo (name) VALUES ('bar');")
	assert.NoError(err)
	assert.NotNil(res)

	rows, err := db.Query("SELECT name FROM foo WHERE name = 'bar';")
	assert.NoError(err)
	assert.NotNil(rows)

	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		assert.NoError(err)
	}

	assert.Equal("bar", name)
}

func TestDriver_Transaction(t *testing.T) {
	assert := require.New(t)
	tempDir, err := ioutil.TempDir(os.TempDir(), "tinydb")
	assert.NoError(err)

	db, err := sql.Open("tinydb", tempDir)
	assert.NoError(err)
	assert.NotNil(db)

	res, err := db.Exec("CREATE TABLE foo (name text);")
	assert.NoError(err)
	assert.NotNil(res)

	tx, err := db.Begin()
	assert.NoError(err)

	res, err = tx.Exec("INSERT INTO foo (name) VALUES ('bar');")
	assert.NoError(err)
	assert.NotNil(res)

	rows, err := tx.Query("SELECT name FROM foo WHERE name = 'bar';")
	assert.NoError(err)
	assert.NotNil(rows)

	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		assert.NoError(err)
	}

	assert.Equal("bar", name)

	assert.NoError(tx.Commit())
}
