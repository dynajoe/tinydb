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
}

// Engine holds metadata and indexes about the database
type Engine struct {
	log          *log.Logger
	config       *Config
	connectCount int
	wal          *storage.WAL
	adminLock    *sync.Mutex

	pagerPool *PagerPool
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
	wal, err := storage.OpenWAL(dbPath, config.PageSize)
	if err != nil {
		return nil, err
	}

	return &Engine{
		config:    config,
		log:       logger,
		wal:       wal,
		adminLock: &sync.Mutex{},
		pagerPool: &PagerPool{
			pager: p,
			cond:  sync.NewCond(&sync.Mutex{}),
		},
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

func (e *Engine) GetPager(connection *Connection, mode pager.Mode) (pager.Pager, error) {
	return e.pagerPool.Acquire(connection.id, mode)
}

func (e *Engine) ReturnPager(connection *Connection) {
	e.pagerPool.Release(connection.id)
}

type PagerPool struct {
	cond    *sync.Cond
	ownerID int
	pager   pager.Pager
}

func (p *PagerPool) Acquire(id int, mode pager.Mode) (pager.Pager, error) {
	p.cond.L.Lock()

	// Already own the pager
	if p.ownerID == id {
		if mode == pager.ModeWrite {
			p.pager.SetMode(mode)
		}
		p.cond.L.Unlock()
		return p.pager, nil
	}

	for p.ownerID != 0 {
		// Wait for owner to be 0
		p.cond.Wait()
	}

	p.ownerID = id
	p.cond.L.Unlock()
	p.pager.SetMode(mode)
	return p.pager, nil
}

func (p *PagerPool) Release(id int) {
	p.cond.L.Lock()
	if p.ownerID == id {
		p.ownerID = 0
		p.pager.SetMode(pager.ModeRead)
		p.cond.L.Unlock()
		p.cond.Signal()
	}
}
