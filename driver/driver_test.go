package driver

import (
	"os"
	"testing"
	"time"

	"database/sql"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/test/bufconn"

	"github.com/joeandaverde/tinydb/internal/backend"
	"github.com/joeandaverde/tinydb/internal/server"
)

type DriverTestSuite struct {
	suite.Suite
	a          *require.Assertions
	driverName string
	dsn        string
	cleanup    func()
}

func (s *DriverTestSuite) SetupTest() {
	s.NoError(os.MkdirAll(".tinydb-test", os.ModePerm))

	tempDir, err := os.MkdirTemp(".tinydb-test", "driver-test-"+time.Now().String()+"*")
	s.NoError(err)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	ln := bufconn.Listen(1024)

	engine, err := backend.Start(logger, backend.Config{
		DataDir:  tempDir,
		PageSize: 4096,
	})
	if err != nil {
		s.FailNow("unable to start test db engine", err)
	}

	// start serving in memory
	dbServer := server.NewServer(logger, server.Config{MaxRecvSize: 4096})
	go dbServer.Serve(ln, engine)

	// for testing we register a unique instance of a driver
	s.driverName = uuid.New().String()
	s.dsn = uuid.New().String()

	sql.Register(s.driverName, &TinyDBDriver{
		testDialer: ln.Dial,
	})

	s.cleanup = func() {
		dbServer.Shutdown()
		ln.Close()
	}
}

func TestDriverTestSuite(t *testing.T) {
	suite.Run(t, new(DriverTestSuite))
}

func (s *DriverTestSuite) TestDriver_Exec() {
	db, err := sql.Open(s.driverName, s.dsn)
	s.NoError(err)
	s.NotNil(db)

	res, err := db.Exec("CREATE TABLE foo (name text);")
	s.NoError(err)
	s.NotNil(res)

	res, err = db.Exec("INSERT INTO foo (name) VALUES ('bar');")
	s.NoError(err)
	s.NotNil(res)

	rows, err := db.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.NoError(err)
	s.NotNil(rows)

	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		s.NoError(err)
	}

	s.Equal("bar", name)
}

func (s *DriverTestSuite) TestDriver_Transaction() {
	db, err := sql.Open(s.driverName, s.dsn)
	s.NoError(err)
	s.NotNil(db)

	res, err := db.Exec("CREATE TABLE foo (name text);")
	s.NoError(err)
	s.NotNil(res)

	tx, err := db.Begin()
	s.NoError(err)

	res, err = tx.Exec("INSERT INTO foo (name) VALUES ('bar');")
	s.NoError(err)
	s.NotNil(res)

	rows, err := tx.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.NoError(err)
	s.NotNil(rows)

	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		s.NoError(err)
	}

	s.Equal("bar", name)

	s.NoError(tx.Commit())

	rows, err = db.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.NoError(err)
	s.NotNil(rows)
	var committedName string
	for rows.Next() {
		err = rows.Scan(&committedName)
		s.NoError(err)
	}
	s.Equal("bar", committedName)
}

func (s *DriverTestSuite) TestDriver_Transaction_Rollback() {
	db, err := sql.Open(s.driverName, s.dsn)
	s.NoError(err)
	s.NotNil(db)

	res, err := db.Exec("CREATE TABLE foo (name text);")
	s.NoError(err)
	s.NotNil(res)

	tx, err := db.Begin()
	s.NoError(err)

	rows, err := tx.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.NoError(err)
	s.False(rows.Next())

	res, err = tx.Exec("INSERT INTO foo (name) VALUES ('bar');")
	s.NoError(err)
	s.NotNil(res)

	rows, err = tx.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.NoError(err)
	s.NotNil(rows)

	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		s.NoError(err)
	}
	s.Equal("bar", name)

	s.NoError(tx.Rollback())

	rows, err = db.Query("SELECT name FROM foo WHERE name = 'bar';")
	s.NoError(err)
	s.False(rows.Next())
}
