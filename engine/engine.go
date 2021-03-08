package engine

import (
	"errors"
	"path"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
)

// Config describes the configuration for the database
type Config struct {
	DataDir  string `yaml:"data_directory"`
	PageSize int    `yaml:"page_size"`
	Addr     string `yaml:"listen_address"`
}

// Engine holds metadata and indexes about the database
type Engine struct {
	log          *log.Logger
	config       *Config
	connectCount int
	wal          *storage.WAL
	adminLock    *sync.Mutex
	pagerPool    *pager.PagerPool
}

// Start initializes a new TinyDb database engine
func Start(config *Config) (*Engine, error) {
	log.Infof("Starting database engine [DataDir: %s]", config.DataDir)

	if config.PageSize < 1024 {
		return nil, errors.New("page size must be greater than or equal to 1024")
	}

	logger := log.New()
	dbPath := path.Join(config.DataDir, "tiny.db")

	// Open the main database file
	dbFile, err := storage.OpenDbFile(dbPath, config.PageSize)
	if err != nil {
		return nil, err
	}

	// Pager abstraction over the database file
	p := pager.NewPager(dbFile, dbFile)
	p.SetMode(pager.ModeWrite)
	defer p.SetMode(pager.ModeRead)

	// Brand new database needs at least one page.
	if dbFile.TotalPages() == 0 {
		// Initialize the first page
		if _, err := p.Allocate(pager.PageTypeLeaf); err != nil {
			return nil, err
		} else if err := p.Flush(); err != nil {
			return nil, err
		}
	}

	// Initialize WAL
	wal, err := storage.OpenWAL(dbFile)
	if err != nil {
		return nil, err
	}

	// Use a WAL pager
	walPager := pager.NewPager(wal, wal)

	return &Engine{
		config:    config,
		log:       logger,
		wal:       wal,
		adminLock: &sync.Mutex{},
		pagerPool: pager.NewPool(walPager),
	}, nil
}

// Connect establishes a new connection to the database engine
func (e *Engine) Connect() *Connection {
	e.log.Debug("connect")
	e.adminLock.Lock()
	defer e.adminLock.Unlock()

	e.connectCount++
	return &Connection{
		id:     e.connectCount,
		mu:     &sync.Mutex{},
		flags:  &virtualmachine.Flags{AutoCommit: true},
		engine: e,
	}
}

// getPager gets a pager from the available pool ensuring isolation
func (e *Engine) getPager(connection *Connection, mode pager.Mode) (pager.Pager, error) {
	e.log.Debug("getPager", "connection_id", connection.id)
	return e.pagerPool.Acquire(connection.id, mode)
}

// ReturnPager returns a pager to the available pool
func (e *Engine) returnPager(connection *Connection) {
	e.log.Debug("returnPager", "connection_id", connection.id)
	e.pagerPool.Release(connection.id)
}
