package engine

import (
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
}

// Engine holds metadata and indexes about the database
type Engine struct {
	Indexes           map[string]*btree.BTree
	Tables            map[string]metadata.TableDefinition
	Log               *log.Logger
	Config            *Config
	Pager             storage.Pager
	adminLock         sync.Mutex
	useVirtualMachine bool
}

// ColumnList represents a list of columns of a result set
type ColumnList []string

// ResultSet is the result of a query; rows are provided asynchronously
type ResultSet struct {
	Columns ColumnList
	Rows    <-chan Row
	Error   <-chan error
}

// Row is a row in a result
type Row struct {
	Data []interface{}
}

// Start initializes a new TinyDb database engine
func Start(config *Config) *Engine {
	log.Infof("Starting database engine [DataDir: %s]", config.DataDir)

	tables := loadTableDefinitions(config)
	indexes := buildIndexes(config, tables)
	logger := log.New()
	pager, err := storage.Open(path.Join(config.DataDir, "tiny.db"))
	if err != nil {
		panic("failed to open database")
	}

	return &Engine{
		Tables:            tables,
		Indexes:           indexes,
		Config:            config,
		Log:               logger,
		Pager:             pager,
		useVirtualMachine: config.UseVirtualMachine,
	}
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

	program := virtualmachine.NewProgram(e.Pager, instructions)
	rowChan := make(chan Row)
	errChan := make(chan error, 1)

	go func() {
		if err := program.Run(); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer close(rowChan)
		for r := range program.Results() {
			rowChan <- Row{
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
