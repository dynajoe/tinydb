package engine

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	assert := assert.New(t)

	testDir, err := ioutil.TempDir(os.TempDir(), "tinydb")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(testDir)

	engine := Start(testDir)
	strings := []string{"select * from foo"}
	instructions := []instruction{
		{OpInteger, 1, 0, 0, 0},
		{OpString, len(strings[0]), 1, 0, 0},
		{OpNull, 0, 2, 0, 0},
		{OpResultRow, 0, 3, 0, 0},
		{OpInteger, 2, 0, 0, 0},
		{OpResultRow, 0, 3, 0, 0},
		{OpInteger, 3, 0, 0, 0},
		{OpResultRow, 0, 3, 0, 0},
		{OpInteger, 4, 0, 0, 0},
		{OpResultRow, 0, 3, 0, 0},
		{OpInteger, 5, 0, 0, 0},
		{OpResultRow, 0, 3, 0, 0},
		{OpHalt, 0, 0, 0, 0},
	}

	testProgram := NewProgram(engine, instructions, strings)
	go testProgram.Run()

	type testrow struct {
		id   int
		sql  string
		null interface{}
	}

	var results []testrow
outer:
	for {
		select {
		case <-time.After(time.Second):
			assert.Fail("row timeout")
		case r := <-testProgram.results:
			if r == nil {
				break outer
			}
			results = append(results, testrow{
				id:   r[0].(int),
				sql:  r[1].(string),
				null: r[2],
			})
		}
	}

	assert.Len(results, 5)
	for i, r := range results {
		assert.Equal(i+1, r.id)
		assert.Equal(strings[0], r.sql)
		assert.Nil(r.null)
	}
}
