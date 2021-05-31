package backend

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type BackendTestSuite struct {
	suite.Suite
	tempDir string
	backend *Backend
	sqlite  *sql.DB
}

func (s *BackendTestSuite) SetupTest() {
	s.NoError(os.MkdirAll(".tinydb-test", os.ModePerm))

	tempDir, err := os.MkdirTemp(".tinydb-test", "backend-test-*")
	s.NoError(err)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	dbEngine, err := Start(logger, Config{
		DataDir:  tempDir,
		PageSize: 4096,
	})
	s.NoError(err)

	useWAL := true
	params := ""
	if useWAL {
		params = "?cache=shared&mode=rwc&_journal_mode=WAL"
	}

	db, err := sql.Open("sqlite3", path.Join(tempDir, "tiny-test-sqlite.db")+params)
	s.NoError(err)

	s.backend = NewBackend(logger, dbEngine.NewPager())

	s.sqlite = db
}

func TestBackendTestSuite(t *testing.T) {
	suite.Run(t, new(BackendTestSuite))
}

func (s *BackendTestSuite) TestSimple_Btree() {
	s.assertQuery("create table foo (name text)")
	s.assertQuery("BEGIN")
	for i := 0; i < 1000; i++ {
		s.assertQuery(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}
	s.assertQuery("COMMIT")

	rows, err := s.simpleQuery("select * from foo where name = '999'")
	s.NoError(err)

	expectedResults := [][]interface{}{
		{"999"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *BackendTestSuite) TestSimple() {
	s.assertQuery("create table foo (name text)")
	s.assertQuery("insert into foo (name) values ('bar')")

	rows, err := s.simpleQuery("select * from foo")
	s.NoError(err)

	s.NotEmpty(rows)
	s.Equal("bar", rows[0].Data[0].(string))
}

func (s *BackendTestSuite) TestSimple_NoData() {
	s.assertQuery("create table foo (name text)")

	rows, err := s.simpleQuery("select * from foo")
	s.NoError(err)
	s.Empty(rows)
}

func (s *BackendTestSuite) TestSimple_WithFilter() {
	s.assertQuery("create table foo (name text)")
	s.assertQuery("insert into foo (name) values ('bar')")
	s.assertQuery("insert into foo (name) values ('baz')")

	rows, err := s.simpleQuery("select * from foo where name = 'bar'")
	s.NoError(err)

	expectedResults := [][]interface{}{{"bar"}}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *BackendTestSuite) TestSimple_WithFilter2() {
	s.assertQuery("create table foo (name text)")
	s.assertQuery("insert into foo (name) values ('bar')")
	s.assertQuery("insert into foo (name) values ('bam')")
	s.assertQuery("insert into foo (name) values ('baz')")

	rows, err := s.simpleQuery("select * from foo where name = 'baz' OR name = 'bam'")
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

func (s *BackendTestSuite) TestSimple_WithFilter3() {
	s.assertQuery("create table foo (name text)")
	for i := 0; i < 10; i++ {
		s.assertQuery(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	rows, err := s.simpleQuery("select * from foo where (name = '1' OR name = '2') OR name = '7' OR name = '4'")
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

func (s *BackendTestSuite) TestSimple_WithFilter4() {
	s.assertQuery("create table foo (name text)")
	for i := 1; i <= 10; i++ {
		s.assertQuery(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	rows, err := s.simpleQuery("select * from foo where name = '1' AND name != '2'")
	s.NoError(err)

	expectedResults := [][]interface{}{
		{"1"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *BackendTestSuite) TestSimple_WithFilter_ComboOrAnd() {
	s.assertQuery("create table foo (name text)")
	for i := 0; i < 10; i++ {
		s.assertQuery(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	rows, err := s.simpleQuery("select * from foo where (name = '1' AND name != '2') OR name = '3'")
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

func (s *BackendTestSuite) TestSimple_WithFilter_ComboOrAndGrouping() {
	s.assertQuery("create table foo (name text)")
	for i := 0; i < 10; i++ {
		s.assertQuery(fmt.Sprintf("insert into foo (name) values ('%d')", i))
	}

	rows, err := s.simpleQuery("select * from foo where name = '1' AND (name != '2' OR name = '3')")
	s.NoError(err)

	expectedResults := [][]interface{}{
		{"1"},
	}
	s.Len(rows, len(expectedResults))
	for i, e := range expectedResults {
		s.Equal(e, rows[i].Data)
	}
}

func (s *BackendTestSuite) assertQuery(query string) {
	_, err := s.sqlite.Exec(query)
	s.NoError(err)

	_, err = s.simpleQuery(query)
	s.NoError(err)
}

func (s *BackendTestSuite) simpleQuery(query string) ([]*Row, error) {
	stmt, err := s.backend.Prepare(query)
	if err != nil {
		return nil, err
	}

	proc, err := s.backend.Exec(context.Background(), stmt)
	if err != nil {
		return nil, err
	}

	var rows []*Row
	for {
		select {
		case r, ok := <-proc.Output:
			if ok {
				rows = append(rows, &Row{Data: r.Data})
			}
		case err := <-proc.Exit:
			if err != nil {
				return nil, err
			}
			return rows, nil
		}
	}
}
