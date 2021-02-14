package engine

import (
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
	expectedResults := [][]interface{}{{"bam", "baz"}}
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

// func (s *VMTestSuite) TestCreateTable() {
// 	createSQL := "CREATE TABLE company (company_id int PRIMARY KEY, company_name text);"
// 	stmt, err := tsql.Parse(createSQL)
// 	s.NoError(err)
//
// 	createTableStatement, ok := stmt.(*ast.CreateTableStatement)
// 	s.True(ok)
//
// 	instructions := CreateTableInstructions(createTableStatement)
// 	program := NewProgram(s.engine, instructions)
// 	program.Run()
//
// 	tableDefinition, err := s.engine.GetTableDefinition("company")
// 	s.NoError(err)
// 	s.Len(tableDefinition.Columns, 2)
// }
//
// func (s *VMTestSuite) TestInsert() {
// 	_, err := interpret.Execute(s.engine, "CREATE TABLE company (company_id int PRIMARY KEY, company_name text, description text);")
// 	s.NoError(err)
//
// 	insertSQL := "INSERT INTO company (company_id, company_name, description) VALUES (99, 'hashicorp', NULL)"
// 	stmt, err := tsql.Parse(insertSQL)
// 	s.NoError(err)
//
// 	insertStmt, ok := stmt.(*ast.InsertStatement)
// 	s.True(ok)
//
// 	instructions := InsertInstructions(s.engine, insertStmt)
// 	program := NewProgram(s.engine, instructions)
// 	err = program.Run()
// 	s.NoError(err)
//
// 	Ensure the row was inserted
// 	result, err := interpret.Execute(s.engine, "SELECT * FROM company")
// 	s.NoError(err)
// 	select {
// 	case <-time.After(time.Second):
// 		s.Fail("test timeout")
// 	case row := <-result.Rows:
// 		s.EqualValues(row.Data[0], 99)
// 		s.Equal(row.Data[1], "hashicorp")
// 		s.Nil(row.Data[2])
// 	}
// }
//
// func (s *VMTestSuite) TestSelect() {
// 	_, err := interpret.Execute(s.engine, "CREATE TABLE company (company_id int PRIMARY KEY, company_name text, description text);")
// 	s.NoError(err)
// 	_, err = interpret.Execute(s.engine, "INSERT INTO company (company_id, company_name, description) VALUES (99, 'hashicorp', NULL)")
// 	s.NoError(err)
//
// 	stmt, err := tsql.Parse("SELECT * FROM company")
// 	s.NoError(err)
// 	selectStmt, ok := stmt.(*ast.SelectStatement)
// 	s.True(ok)
//
// 	instructions := SelectInstructions(s.engine, selectStmt)
// 	program := NewProgram(s.engine, instructions)
// 	go program.Run()
//
// 	select {
// 	case <-time.After(time.Second):
// 		s.Fail("test timeout")
// 	case row := <-program.Results():
// 		if row == nil {
// 			s.FailNow("expected a row")
// 		}
// 		s.EqualValues(row[0], 99)
// 		s.Equal(row[1], "hashicorp")
// 		s.Nil(row[2])
// 	}
// }
