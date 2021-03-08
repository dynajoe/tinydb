package engine

import (
	"errors"
	"path"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/metadata"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
	"github.com/joeandaverde/tinydb/tsql"
)

// Config describes the configuration for the database
type Config struct {
	DataDir  string `yaml:"data_directory"`
	PageSize int    `yaml:"page_size"`
}

// Engine holds metadata and indexes about the database
type Engine struct {
	Tables map[string]metadata.TableDefinition
	Log    *log.Logger
	Config *Config
	WAL    *storage.WAL

	pager             storage.ReservablePager
	adminLock         sync.Mutex
	useVirtualMachine bool
}

type Connection struct {
	autoCommit    bool
	flags         *virtualmachine.Flags
	engine        *Engine
	reservedPager *storage.ReservedPager
}

// ColumnList represents a list of columns of a result set
type ColumnList []string

// ResultSet is the result of a query; rows are provided asynchronously
type ResultSet struct {
	Columns ColumnList
	Rows    <-chan *Row
	Error   <-chan error
}

// Row is a row in a result
type Row struct {
	Data []interface{}
}

// Start initializes a new TinyDb database engine
func Start(config *Config) (*Engine, error) {
	log.Infof("Starting database engine [DataDir: %s]", config.DataDir)

	if config.PageSize < 1024 {
		return nil, errors.New("page size must be greater than or equal to 1024")
	}

	tables := loadTableDefinitions(config)
	logger := log.New()
	dbPath := path.Join(config.DataDir, "tiny.db")

	// Open the main database file
	dbFile, err := storage.OpenDbFile(dbPath, config.PageSize)
	if err != nil {
		return nil, err
	}

	// Pager abstraction over the database file
	reservablePager := storage.NewReservablePager(dbFile, dbFile)

	// Brand new database needs at least one page.
	if dbFile.TotalPages() == 0 {
		reserved := reservablePager.Reserve(storage.ModeWrite)
		defer reserved.Release()
		pager := reserved.Pager()

		// Initialize the first page
		if _, err := pager.Allocate(storage.PageTypeLeaf); err != nil {
			return nil, err
		} else if err := pager.Flush(); err != nil {
			return nil, err
		}
	}

	// Initialize WAL
	wal, err := storage.OpenWAL(dbPath, config.PageSize)
	if err != nil {
		return nil, err
	}

	return &Engine{
		Tables: tables,
		Config: config,
		Log:    logger,
		WAL:    wal,

		pager: reservablePager,
	}, nil
}

func (e *Engine) Connect() *Connection {
	return &Connection{
		flags:  &virtualmachine.Flags{AutoCommit: true},
		engine: e,
	}
}

// Exec executes a command on the database connection
func (c *Connection) Exec(text string) (*ResultSet, error) {
	stmt, err := tsql.Parse(text)
	if err != nil {
		return nil, err
	}

	// If there's no pager reserved, get one.
	if c.reservedPager == nil {
		c.reservedPager = c.engine.pager.Reserve(storage.ModeRead)
	}

	// If this query mutates, upgrade the pager to a writer.
	if stmt.Mutates() {
		// This will block until all readers and writers are finished.
		c.reservedPager.Upgrade()
	}

	preparedStmt, err := virtualmachine.Prepare(stmt, c.reservedPager.Pager())
	if err != nil {
		return nil, err
	}

	program := virtualmachine.NewProgram(c.flags, c.reservedPager.Pager(), preparedStmt)
	rowChan := make(chan *Row)
	errChan := make(chan error, 1)

	go func() {
		if err := program.Run(); err != nil {
			// on error force rollback of any in progress transactions, ignore errors.
			_ = c.handleTx(true)
			errChan <- err
			return
		}

		if err := c.handleTx(false); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer close(rowChan)
		for r := range program.Results() {
			rowChan <- &Row{
				Data: r,
			}
		}
	}()

	return &ResultSet{
		Columns: preparedStmt.Columns,
		Rows:    rowChan,
		Error:   errChan,
	}, nil
}

func (c *Connection) handleTx(forceRollback bool) error {
	pager := c.reservedPager.Pager()

	if forceRollback {
		c.flags.AutoCommit = true
		c.flags.Rollback = false
		pager.Reset()
		c.reservedPager.Release()
		c.reservedPager = nil
		return nil
	}

	// update auto commit flag
	c.autoCommit = c.flags.AutoCommit

	if c.autoCommit {
		if c.flags.Rollback {
			pager.Reset()
			c.flags.Rollback = false
		} else {
			if err := pager.Flush(); err != nil {
				return err
			}
		}
		c.reservedPager.Release()
		c.reservedPager = nil
	}

	return nil
}

func (e *Engine) reload() {
	e.loadTables()
}
