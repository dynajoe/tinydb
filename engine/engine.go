package engine

import (
	"errors"
	"path"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
)

// Config describes the configuration for the database
type Config struct {
	DataDir  string `yaml:"data_directory"`
	PageSize int    `yaml:"page_size"`
}

// Engine holds metadata and indexes about the database
type Engine struct {
	log          *log.Logger
	config       *Config
	connectCount int
	pager        storage.ReservablePager
	wal          *storage.WAL
	adminLock    *sync.Mutex
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
		config:    config,
		log:       logger,
		wal:       wal,
		adminLock: &sync.Mutex{},
		pager:     reservablePager,
	}, nil
}

// Connect establishes a new connection to the database engine
func (e *Engine) Connect() *Connection {
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
