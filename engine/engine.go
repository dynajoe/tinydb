package engine

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/btree"
)

type (
	indexedField struct {
		value  string
		offset int
	}

	pkJob struct {
		table      *TableDefinition
		fieldIndex int
		result     *btree.BTree
	}

	// Config describes the configuration for the database
	Config struct {
		BasePath string
	}

	// Engine holds metadata and indexes about the database
	Engine struct {
		Indexes map[string]*btree.BTree
		Tables  map[string]*TableDefinition
		Config  *Config
	}
)

func (f *indexedField) Less(than btree.Item) bool {
	return f.value < than.(*indexedField).value
}

// Start initializes a new TinyDb database engine
func Start(basePath string) *Engine {
	log.Infof("Starting database engine [BasePath: %s]", basePath)

	config := &Config{
		BasePath: basePath,
	}

	tables := loadTableDefinitions(config)
	indexes := buildIndexes(config, tables)

	return &Engine{
		Tables:  tables,
		Indexes: indexes,
		Config:  config,
	}
}

// Execute runs a statement against the database engine
func Execute(engine *Engine, text string) (*ResultSet, error) {
	log.Debug("EXEC: ", text)
	statement, err := Parse(strings.TrimSpace(text))

	if err != nil {
		return nil, err
	}

	if statement != nil {
		return executeStatement(engine, statement)
	}

	return nil, fmt.Errorf("Unable to parse statement %s", text)
}

func executeStatement(engine *Engine, statement Statement) (*ResultSet, error) {
	startingTime := time.Now().UTC()
	defer (func() {
		duration := time.Now().UTC().Sub(startingTime)
		fmt.Printf("\nDuration: %s\n", duration)
	})()

	switch s := (statement).(type) {
	case *CreateTableStatement:
		if _, err := createTable(engine, s); err != nil {
			return nil, err
		}

		engine.Tables = loadTableDefinitions(engine.Config)

		return emptyResultSet(), nil
	case *InsertStatement:
		_, result, err := doInsert(engine, s)

		if err != nil {
			return nil, err
		}

		return result, nil
	case *SelectStatement:
		return doSelect(engine, s)
	}

	return nil, fmt.Errorf("Unexpected statement type")
}

func newTableScanner(config *Config, tableName string) (*csv.Reader, error) {
	csvFile, err := os.Open(filepath.Join(config.BasePath, "tsql_data", tableName, "data.csv"))

	if err != nil {
		return nil, err
	}

	tableCsv := csv.NewReader(bufio.NewReader(csvFile))

	return tableCsv, nil
}

func buildIndex(config *Config, job *pkJob) {
	btree := btree.New(5)

	csvReader, err := newTableScanner(config, job.table.Name)

	if err != nil {
		panic("unable to build index")
	}

	rowCount := 0
	for {
		data, err := csvReader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		rowCount++

		btree.Insert(&indexedField{
			value:  data[job.fieldIndex],
			offset: rowCount,
		})
	}

	job.result = btree
}

func buildIndexes(config *Config, m map[string]*TableDefinition) map[string]*btree.BTree {
	indexes := make(map[string]*btree.BTree)
	results := make(chan *pkJob)

	var wg sync.WaitGroup

	for _, t := range m {
		for i, c := range t.Columns {
			if c.PrimaryKey {
				wg.Add(1)
				go func(i int, t *TableDefinition) {
					defer wg.Done()
					job := &pkJob{fieldIndex: i, table: t}
					buildIndex(config, job)
					results <- job
				}(i, t)
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

func loadTableDefinitions(config *Config) map[string]*TableDefinition {
	tableDefinitions := make(map[string]*TableDefinition)

	filepath.Walk(path.Join(config.BasePath, "tsql_data"), func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(p, "metadata.json") {
			data, err := ioutil.ReadFile(p)

			if err != nil {
				panic("unable to load tables")
			}

			var tableDefinition TableDefinition
			err = json.Unmarshal(data, &tableDefinition)

			if err != nil {
				panic("unable to load tables")
			}

			tableDefinitions[tableDefinition.Name] = &tableDefinition
		}

		return nil
	})

	return tableDefinitions
}

func newExecutionEnvironment(engine *Engine, tables []TableAlias) (*ExecutionEnvironment, error) {
	columnLookup := make(map[string]*ColumnReference)
	tableMetadata := make(map[string]*TableDefinition)
	allMetadata := make([]*TableDefinition, len(tables))

	i := 0
	for _, tableAlias := range tables {
		metadata, _ := engine.Tables[tableAlias.name]

		for _, c := range metadata.Columns {
			columnLookup[fmt.Sprintf("%s.%s", tableAlias.alias, c.Name)] = &ColumnReference{
				table:      tableAlias.name,
				alias:      tableAlias.alias,
				index:      i,
				definition: c,
			}

			i++
		}

		tableMetadata[tableAlias.alias] = metadata
		allMetadata = append(allMetadata, metadata)
	}

	return &ExecutionEnvironment{
		Tables:       tableMetadata,
		ColumnLookup: columnLookup,
		Engine:       engine,
	}, nil
}
