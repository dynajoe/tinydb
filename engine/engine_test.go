package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	// For connection to sqlite
	"github.com/joeandaverde/tinydb/internal/pager"
	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/suite"
)

type VMTestSuite struct {
	suite.Suite
	tempDir string
	engine  *Engine
	conn    *Connection
	sqlite  *sql.DB
}

func (s *VMTestSuite) SetupTest() {
	s.tempDir = "/Users/joe/tiny-test/"
	_ = os.RemoveAll(s.tempDir)
	s.NoError(os.MkdirAll(s.tempDir, os.ModePerm))

	engine, err := Start(&Config{
		DataDir:  s.tempDir,
		PageSize: 4096,
	})
	s.NoError(err)

	useWAL := true
	params := ""
	if useWAL {
		params = "?cache=shared&mode=rwc&_journal_mode=WAL"
	}

	db, err := sql.Open("sqlite3", s.tempDir+"tiny-test-sqlite.db"+params)
	s.NoError(err)

	s.engine = engine
	s.conn = NewConnection(engine.log, pager.NewPager(engine.wal), nil)

	s.sqlite = db
}

func TestVMTestSuite(t *testing.T) {
	suite.Run(t, new(VMTestSuite))
}

func (s *VMTestSuite) TestSimple_Btree() {
	s.AssertCommand("create table foo (name text)")
	s.AssertCommand("BEGIN")
	for i := 0; i < 1000; i++ {
		s.AssertCommand(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}
	s.AssertCommand("COMMIT")

	rows := s.simpleQuery("select * from foo where name = '999'")

	expectedResults := [][]interface{}{
		{"999"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *VMTestSuite) TestSimple() {
	s.AssertCommand("create table foo (name text)")
	s.AssertCommand("insert into foo (name) values ('bar')")

	rows := s.simpleQuery("select * from foo")

	s.NotEmpty(rows)
	s.Equal("bar", rows[0].Data[0].(string))
}

func (s *VMTestSuite) TestSimple_WithFilter() {
	s.AssertCommand("create table foo (name text)")
	s.AssertCommand("insert into foo (name) values ('bar')")
	s.AssertCommand("insert into foo (name) values ('baz')")

	rows := s.simpleQuery("select * from foo where name = 'bar'")

	expectedResults := [][]interface{}{{"bar"}}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *VMTestSuite) TestSimple_WithFilter2() {
	s.AssertCommand("create table foo (name text)")
	s.AssertCommand("insert into foo (name) values ('bar')")
	s.AssertCommand("insert into foo (name) values ('bam')")
	s.AssertCommand("insert into foo (name) values ('baz')")

	rows := s.simpleQuery("select * from foo where name = 'baz' OR name = 'bam'")

	expectedResults := [][]interface{}{
		{"bam"},
		{"baz"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *VMTestSuite) TestSimple_WithFilter3() {
	s.AssertCommand("create table foo (name text)")
	for i := 0; i < 10; i++ {
		s.AssertCommand(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	rows := s.simpleQuery("select * from foo where (name = '1' OR name = '2') OR name = '7' OR name = '4'")

	expectedResults := [][]interface{}{
		{"1"},
		{"2"},
		{"4"},
		{"7"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *VMTestSuite) TestSimple_WithFilter4() {
	s.AssertCommand("create table foo (name text)")
	for i := 1; i <= 10; i++ {
		s.AssertCommand(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	rows := s.simpleQuery("select * from foo where name = '1' AND name != '2'")

	expectedResults := [][]interface{}{
		{"1"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *VMTestSuite) TestSimple_WithFilter_ComboOrAnd() {
	s.AssertCommand("create table foo (name text)")
	for i := 0; i < 10; i++ {
		s.AssertCommand(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	rows := s.simpleQuery("select * from foo where (name = '1' AND name != '2') OR name = '3'")

	expectedResults := [][]interface{}{
		{"1"},
		{"3"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *VMTestSuite) TestSimple_WithFilter_ComboOrAndGrouping() {
	s.AssertCommand("create table foo (name text)")
	for i := 0; i < 10; i++ {
		s.AssertCommand(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	rows := s.simpleQuery("select * from foo where name = '1' AND (name != '2' OR name = '3')")

	expectedResults := [][]interface{}{
		{"1"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *VMTestSuite) AssertCommand(cmd string) {
	_, err := s.sqlite.Exec(cmd)
	s.NoError(err)

	_ = s.simpleQuery(cmd)
}

func (s *VMTestSuite) simpleQuery(query string) []*Row {
	stmt, err := s.conn.prepare(query)
	s.NoError(err)

	program, err := s.conn.exec(context.Background(), stmt)
	s.NoError(err)

	var rows []*Row
	for {
		select {
		case <-s.conn.queryComplete:
			return rows
		case r, ok := <-program.Output():
			if ok {
				rows = append(rows, &Row{Data: r.Data})
			}
		}
	}
}
