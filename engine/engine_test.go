package engine

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type VMTestSuite struct {
	suite.Suite
	tempDir string
	engine  *Engine
}

func (s *VMTestSuite) SetupTest() {
	tempDir, err := ioutil.TempDir(os.TempDir(), "tinydb")
	s.tempDir = tempDir
	if err != nil {
		s.Error(err)
	}
	s.engine = Start(&Config{
		DataDir:           tempDir,
		UseVirtualMachine: true,
	})
}

func (s *VMTestSuite) TearDownTest() {
	if s.tempDir != "" {
		_ = os.RemoveAll(s.tempDir)
	}
}

func TestVMTestSuite(t *testing.T) {
	suite.Run(t, new(VMTestSuite))
}

func (s *VMTestSuite) TestSimple() {
	s.AssertCommand("create table foo (name text)")
	s.AssertCommand("insert into foo (name) values ('bar')")

	results, err := s.engine.Command("select * from foo")
	s.NoError(err)
	rows := collectRows(results)

	s.NotEmpty(rows)
	s.Equal("bar", rows[0].Data[0].(string))
}

func (s *VMTestSuite) TestSimple_WithFilter() {
	s.AssertCommand("create table foo (name text)")
	s.AssertCommand("insert into foo (name) values ('bar')")
	s.AssertCommand("insert into foo (name) values ('baz')")

	results, err := s.engine.Command("select * from foo where name = 'bar'")
	s.NoError(err)
	rows := collectRows(results)
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

	results, err := s.engine.Command("select * from foo where name = 'baz' OR name = 'bam'")
	s.NoError(err)
	rows := collectRows(results)
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

	results, err := s.engine.Command("select * from foo where (name = '1' OR name = '2') OR name = '7' OR name = '4'")
	s.NoError(err)
	rows := collectRows(results)
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
	for i := 0; i < 10; i++ {
		s.AssertCommand(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	results, err := s.engine.Command("select * from foo where name = '1' AND name != '2'")
	s.NoError(err)
	rows := collectRows(results)
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

	results, err := s.engine.Command("select * from foo where (name = '1' AND name != '2') OR name = '3'")
	s.NoError(err)
	rows := collectRows(results)
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

	results, err := s.engine.Command("select * from foo where name = '1' AND (name != '2' OR name = '3')")
	s.NoError(err)
	rows := collectRows(results)
	expectedResults := [][]interface{}{
		{"1"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *VMTestSuite) AssertCommand(cmd string) {
	results, err := s.engine.Command(cmd)
	s.NoError(err)
	collectRows(results)
}

func collectRows(rs *ResultSet) []Row {
	var rows []Row
	for r := range rs.Rows {
		rows = append(rows, r)
	}
	return rows
}
