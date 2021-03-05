package engine

import (
	"errors"
	"path"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/btree"
	"github.com/joeandaverde/tinydb/internal/metadata"
	"github.com/joeandaverde/tinydb/internal/prepare"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
)

// Config describes the configuration for the database
type Config struct {
	DataDir           string `yaml:"data_directory"`
	Addr              string `yaml:"listen"`
	UseVirtualMachine bool   `yaml:"use_virtual_machine"`
	PageSize          int    `yaml:"page_size"`
}

// Engine holds metadata and indexes about the database
type Engine struct {
	Indexes           map[string]*btree.BTree
	Tables            map[string]metadata.TableDefinition
	Log               *log.Logger
	Config            *Config
	Pager             storage.Pager
	WAL               *storage.WAL
	adminLock         sync.Mutex
	useVirtualMachine bool
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
	indexes := buildIndexes(config, tables)
	logger := log.New()
	dbPath := path.Join(config.DataDir, "tiny.db")

	pager, err := storage.Open(dbPath, config.PageSize)
	if err != nil {
		return nil, err
	}

	// Initialize WAL.
	wal, err := storage.OpenWAL(dbPath, config.PageSize)
	if err != nil {
		return nil, err
	}

	return &Engine{
		Tables:            tables,
		Indexes:           indexes,
		Config:            config,
		Log:               logger,
		Pager:             pager,
		WAL:               wal,
		useVirtualMachine: config.UseVirtualMachine,
	}, nil
}

// Command executes a command against the database engine
func (e *Engine) Command(text string) (*ResultSet, error) {
	e.Log.Debug("EXEC: ", text)

	// TODO: Figure out how not to pass Pager around or even expose on the engine.

	instructions, err := prepare.Prepare(text, e.Pager)
	if err != nil {
		return nil, err
	}

	// TODO: think about the concurrency model some.

	program := virtualmachine.NewProgram(e.Pager, e.WAL, instructions)
	rowChan := make(chan *Row)
	errChan := make(chan error, 1)

	go func() {
		if err := program.Run(); err != nil {
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
		// TODO: wonder how the column name metadata should be returned?
		// The prepared program should know that.
		Columns: []string{},
		Rows:    rowChan,
		Error:   errChan,
	}, nil
}

func (e *Engine) reload() {
	e.loadTables()
	e.loadIndexes()
}
