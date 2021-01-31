package engine

import (
	"path"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/btree"
	"github.com/joeandaverde/tinydb/internal/storage"
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
	Tables            map[string]TableDefinition
	Log               *log.Logger
	Config            *Config
	Pager             *storage.Pager
	adminLock         sync.Mutex
	useVirtualMachine bool
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

func (e *Engine) reload() {
	e.loadTables()
	e.loadIndexes()
}
