package driver

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/joeandaverde/tinydb/engine"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/test/bufconn"
)

type DriverTestSuite struct {
	suite.Suite
	a          *require.Assertions
	driverName string
	dsn        string
	cleanup    func()
}

func (s *DriverTestSuite) SetupTest() {
	s.a = require.New(s.T())
	tempDir, err := ioutil.TempDir(os.TempDir(), "tinydb")
	if err != nil {
		s.a.FailNow("unable to create temporary test db path", err)
	}

	ln := bufconn.Listen(1024)

	server, err := engine.Start(&engine.Config{
		DataDir:          tempDir,
		PageSize:         4096,
		MaxReceiveBuffer: 4096,
		LogLevel:         logrus.DebugLevel,
	})
	if err != nil {
		s.a.FailNow("unable to start test db engine", err)
	}

	// start serving in memory
	ctx, cancel := context.WithCancel(context.Background())
	go engine.Serve(ctx, ln, server)

	// for testing we register a unique instance of a driver
	s.driverName = uuid.New().String()
	s.dsn = uuid.New().String()

	sql.Register(s.driverName, &TinyDBDriver{
		testDialer: ln.Dial,
	})

	s.cleanup = func() {
		ln.Close()
		cancel()
	}
}

func TestDriverTestSuite(t *testing.T) {
	suite.Run(t, new(DriverTestSuite))
}

func (s *DriverTestSuite) TestDriver_Exec() {
	db, err := sql.Open(s.driverName, s.dsn)
	s.a.NoError(err)
	s.a.NotNil(db)

	res, err := db.Exec("CREATE TABLE foo (name text);")
	s.a.NoError(err)
	s.a.NotNil(res)

	res, err = db.Exec("INSERT INTO foo (name) VALUES ('bar');")
	s.a.NoError(err)
	s.a.NotNil(res)

	rows, err := db.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.a.NoError(err)
	s.a.NotNil(rows)

	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		s.a.NoError(err)
	}

	s.a.Equal("bar", name)
}

func (s *DriverTestSuite) TestDriver_Transaction() {
	db, err := sql.Open(s.driverName, s.dsn)
	s.a.NoError(err)
	s.a.NotNil(db)

	res, err := db.Exec("CREATE TABLE foo (name text);")
	s.a.NoError(err)
	s.a.NotNil(res)

	tx, err := db.Begin()
	s.a.NoError(err)

	res, err = tx.Exec("INSERT INTO foo (name) VALUES ('bar');")
	s.a.NoError(err)
	s.a.NotNil(res)

	rows, err := tx.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.a.NoError(err)
	s.a.NotNil(rows)

	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		s.a.NoError(err)
	}

	s.a.Equal("bar", name)

	s.a.NoError(tx.Commit())

	rows, err = db.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.a.NoError(err)
	s.a.NotNil(rows)
	var committedName string
	for rows.Next() {
		err = rows.Scan(&committedName)
		s.a.NoError(err)
	}
	s.a.Equal("bar", committedName)
}

func (s *DriverTestSuite) TestDriver_Transaction_Rollback() {
	db, err := sql.Open(s.driverName, s.dsn)
	s.a.NoError(err)
	s.a.NotNil(db)

	res, err := db.Exec("CREATE TABLE foo (name text);")
	s.a.NoError(err)
	s.a.NotNil(res)

	tx, err := db.Begin()
	s.a.NoError(err)

	res, err = tx.Exec("INSERT INTO foo (name) VALUES ('bar');")
	s.a.NoError(err)
	s.a.NotNil(res)

	rows, err := tx.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.a.NoError(err)
	s.a.NotNil(rows)

	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		s.a.NoError(err)
	}
	s.a.Equal("bar", name)

	s.a.NoError(tx.Rollback())

	rows, err = db.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.a.NoError(err)
	s.a.False(rows.Next())
}
