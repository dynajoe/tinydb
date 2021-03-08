package engine

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	// For connection to sqlite
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
	s.NoError(os.MkdirAll(s.tempDir, os.ModePerm))

	engine, err := Start(&Config{
		DataDir:  s.tempDir,
		PageSize: 4096,
	})
	s.NoError(err)

	db, err := sql.Open("sqlite3", s.tempDir+"tiny-test-sqlite.db")
	s.NoError(err)

	s.engine = engine
	s.conn = s.engine.Connect()
	s.sqlite = db
}

func (s *VMTestSuite) TearDownTest() {
	if s.tempDir != "" {
		_ = os.RemoveAll(s.tempDir)
	}
}

func TestVMTestSuite(t *testing.T) {
	suite.Run(t, new(VMTestSuite))
}

func (s *VMTestSuite) TestSimple_Btree() {
	s.AssertCommand("create table foo (name text)")
	for i := 0; i < 1000; i++ {
		s.AssertCommand(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	results, err := s.conn.Exec("select * from foo where name = '999'")
	s.NoError(err)

	rows, err := collectRows(results)
	s.NoError(err)

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

	results, err := s.conn.Exec("select * from foo")
	s.NoError(err)

	rows, err := collectRows(results)
	s.NoError(err)

	s.NotEmpty(rows)
	s.Equal("bar", rows[0].Data[0].(string))
}

func (s *VMTestSuite) TestSimple_WithFilter() {
	s.AssertCommand("create table foo (name text)")
	s.AssertCommand("insert into foo (name) values ('bar')")
	s.AssertCommand("insert into foo (name) values ('baz')")

	results, err := s.conn.Exec("select * from foo where name = 'bar'")
	s.NoError(err)

	rows, err := collectRows(results)
	s.NoError(err)

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

	results, err := s.conn.Exec("select * from foo where name = 'baz' OR name = 'bam'")
	s.NoError(err)

	rows, err := collectRows(results)
	s.NoError(err)

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

	results, err := s.conn.Exec("select * from foo where (name = '1' OR name = '2') OR name = '7' OR name = '4'")
	s.NoError(err)

	rows, err := collectRows(results)
	s.NoError(err)

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

	results, err := s.conn.Exec("select * from foo where name = '1' AND name != '2'")
	s.NoError(err)

	rows, err := collectRows(results)
	s.NoError(err)

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

	results, err := s.conn.Exec("select * from foo where (name = '1' AND name != '2') OR name = '3'")
	s.NoError(err)

	rows, err := collectRows(results)
	s.NoError(err)

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

	results, err := s.conn.Exec("select * from foo where name = '1' AND (name != '2' OR name = '3')")
	s.NoError(err)

	rows, err := collectRows(results)
	s.NoError(err)

	expectedResults := [][]interface{}{
		{"1"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *VMTestSuite) AssertCommand(cmd string) {
	//s.sqlite.Exec(cmd)

	results, err := s.conn.Exec(cmd)
	s.NoError(err)
	collectRows(results)
}

func collectRows(rs *ResultSet) ([]*Row, error) {
	var rows []*Row
outer:
	for {
		r := <-rs.Results
		if r == nil {
			break outer
		}
		if r.Error != nil {
			return nil, r.Error
		}
		rows = append(rows, r)
	}

	return rows, nil
}
