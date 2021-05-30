package backend

import (
	"errors"
	"path"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/storage"
)

// Config describes the configuration for the database
type Config struct {
	DataDir  string
	PageSize int
}

// Engine holds metadata and indexes about the database
type Engine struct {
	sync.RWMutex
	log       *log.Logger
	config    Config
	wal       *storage.WAL
	pagerPool *pager.Pool
	txID      uint32
}

// Start initializes a new TinyDb database engine
func Start(log *logrus.Logger, config Config) (*Engine, error) {
	log.Infof("Starting database engine [DataDir: %s]", config.DataDir)

	if config.PageSize < 1024 {
		return nil, errors.New("page size must be greater than or equal to 1024")
	}

	dbPath := path.Join(config.DataDir, "tiny.db")

	// Open the main database file
	dbFile, err := storage.OpenDbFile(dbPath, config.PageSize)
	if err != nil {
		return nil, err
	}

	// Brand new database needs at least one page.
	if dbFile.TotalPages() == 0 {
		if err := pager.Initialize(dbFile); err != nil {
			return nil, err
		}
	}

	// Initialize WAL
	wal, err := storage.OpenWAL(dbFile)
	if err != nil {
		return nil, err
	}

	return &Engine{
		config:    config,
		log:       log,
		wal:       wal,
		pagerPool: pager.NewPool(pager.NewPager(wal)),
	}, nil
}

// TxID provides a new transaction id
func (e *Engine) TxID() uint32 {
	return atomic.AddUint32(&e.txID, 1)
}

func (e *Engine) NewPager() pager.Pager {
	return pager.NewPager(e.wal)
}
