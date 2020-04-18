package engine

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/joeandaverde/tinydb/ast"
	log "github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/btree"
	"github.com/joeandaverde/tinydb/internal/storage"
)

type (
	// ColumnDefinition represents a specification for a column in a table
	ColumnDefinition struct {
		Name       string          `json:"name"`
		Type       storage.SQLType `json:"type"`
		Offset     int             `json:"offset"`
		PrimaryKey bool            `json:"is_primary_key"`
	}

	TableDefinition struct {
		Name     string             `json:"name"`
		Columns  []ColumnDefinition `json:"columns"`
		RootPage int                `json:"root_page"`
	}

	columnLookup struct {
		index  int
		column ColumnDefinition
		alias  ast.TableAlias
	}

	indexedField struct {
		value   string
		offsets []int64
	}

	pkJob struct {
		table      TableDefinition
		fieldIndex int
		result     *btree.BTree
	}

	// Config describes the configuration for the database
	Config struct {
		DataDir string `yaml:"data_directory"`
		Addr    string `yaml:"listen"`
	}

	// Engine holds metadata and indexes about the database
	Engine struct {
		Indexes   map[string]*btree.BTree
		Tables    map[string]TableDefinition
		Log       *log.Logger
		Config    *Config
		Pager     *storage.Pager
		adminLock sync.Mutex
	}

	ExecutionEnvironment struct {
		ColumnLookup map[string]columnLookup
		Tables       map[string]TableDefinition
		Columns      []string
		Indexes      map[string]*btree.BTree
		Engine       *Engine
	}

	RowReader interface {
		Scan() bool
		Read() Row
	}
)

func (f *indexedField) Less(than btree.Item) bool {
	return f.value < than.(*indexedField).value
}

// Start initializes a new TinyDb database engine
func Start(basePath string) *Engine {
	log.Infof("Starting database engine [DataDir: %s]", basePath)

	config := &Config{
		DataDir: basePath,
	}

	tables := loadTableDefinitions(config)
	indexes := buildIndexes(config, tables)
	logger := log.New()
	pager, err := storage.Open(path.Join(config.DataDir, "tiny.db"))
	if err != nil {
		panic("failed to open database")
	}

	return &Engine{
		Tables:  tables,
		Indexes: indexes,
		Config:  config,
		Log:     logger,
		Pager:   pager,
	}
}

type rowGenerator struct {
	csvReader *csv.Reader
	next      []string
}

// Execute runs a statement against the database engine
func (e *Engine) Execute(text string) (*ResultSet, error) {
	startingTime := time.Now().UTC()
	defer func() {
		duration := time.Now().UTC().Sub(startingTime)
		e.Log.Infof("\nDuration: %s\n", duration)
	}()

	e.Log.Debug("EXEC: ", text)

	statement, err := ast.Parse(strings.TrimSpace(text))
	if err != nil {
		return nil, err
	}

	return executeStatement(e, statement)
}

func (e *Engine) loadTables() {
	newTables := loadTableDefinitions(e.Config)
	e.adminLock.Lock()
	e.Tables = newTables
	e.adminLock.Unlock()
}

func (e *Engine) loadIndexes() {
	newIndexes := buildIndexes(e.Config, e.Tables)
	e.adminLock.Lock()
	e.Indexes = newIndexes
	e.adminLock.Unlock()
}

func (e *Engine) reload() {
	e.loadTables()
	e.loadIndexes()
}

func executeStatement(engine *Engine, statement ast.Statement) (*ResultSet, error) {
	switch s := (statement).(type) {
	case *ast.CreateTableStatement:
		if _, err := createTable(engine, s); err != nil {
			return nil, err
		}
		engine.reload()
		return EmptyResultSet(), nil
	case *ast.InsertStatement:
		_, result, err := doInsert(engine, s)
		return result, err
	case *ast.SelectStatement:
		return doSelect(engine, s)
	default:
		return nil, fmt.Errorf("unexpected statement type")
	}
}

func buildIndex(config *Config, job *pkJob) {
	// rowReader, err := newTableScanner(config, job.table.Name)

	// if err != nil {
	// 	panic("unable to build index")
	// }

	b := btree.New(5)
	// for rowReader.Scan() {
	// 	row := rowReader.Read()
	// 	b.Upsert(&indexedField{
	// 		value:   row.Data[job.fieldIndex],
	// 		offsets: []int64{row.Offset},
	// 	}, func(old, new btree.Item) {
	// 		newField := new.(*indexedField)
	// 		newField.offsets = append(old.(*indexedField).offsets, row.Offset)
	// 	})
	// }

	job.result = b
}

func buildIndexes(config *Config, m map[string]TableDefinition) map[string]*btree.BTree {
	indexes := make(map[string]*btree.BTree)
	results := make(chan *pkJob)

	var wg sync.WaitGroup

	for _, t := range m {
		for _, c := range t.Columns {
			if c.PrimaryKey {
				wg.Add(1)
				go func(t TableDefinition, c ColumnDefinition) {
					defer wg.Done()
					job := &pkJob{fieldIndex: c.Offset, table: t}
					buildIndex(config, job)
					results <- job
				}(t, c)
			}
		}
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for job := range results {
		indexes[job.table.Name] = job.result
	}

	return indexes
}

func loadTableDefinitions(config *Config) map[string]TableDefinition {
	tableDefinitions := make(map[string]TableDefinition)
	matches, _ := filepath.Glob(filepath.Join(config.DataDir, "**/metadata.json"))

	for _, p := range matches {
		data, err := ioutil.ReadFile(p)

		if err != nil {
			panic("unable to load tables")
		}

		var tableDefinition TableDefinition
		err = json.Unmarshal(data, &tableDefinition)

		if err != nil {
			panic("unable to load tables")
		}

		tableDefinitions[tableDefinition.Name] = tableDefinition
	}

	return tableDefinitions
}

func newExecutionEnvironment(engine *Engine, tables []ast.TableAlias) (*ExecutionEnvironment, error) {
	colLookup := make(map[string]columnLookup)
	tableMetadata := make(map[string]TableDefinition)
	allMetadata := make([]TableDefinition, len(tables))
	i := 0
	for _, tableAlias := range tables {
		metadata, _ := engine.Tables[tableAlias.Name]

		for _, c := range metadata.Columns {
			colLookup[fmt.Sprintf("%s.%s", tableAlias.Alias, c.Name)] = columnLookup{
				index:  i,
				alias:  tableAlias,
				column: c,
			}
			i++
		}

		tableMetadata[tableAlias.Alias] = metadata
		allMetadata = append(allMetadata, metadata)
	}

	return &ExecutionEnvironment{
		Tables:       tableMetadata,
		ColumnLookup: colLookup,
		Engine:       engine,
	}, nil
}

func (c *ColumnDefinition) DefaultValue() interface{} {
	if c.PrimaryKey {
		return nextKey(c.Name)
	}
	return c.DefaultValue
}
