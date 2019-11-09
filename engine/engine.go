package engine

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Engine struct {
	Indexes map[string]*BTree
	Tables  map[string]*TableMetadata
}

type (
	indexedField struct {
		value  string
		offset int
	}

	pkJob struct {
		table      *TableMetadata
		fieldIndex int
		result     *BTree
	}
)

func Start() *Engine {
	log.Info("Starting database engine")
	tables := loadTables()
	indexes := buildIndexes(tables)

	return &Engine{
		Tables:  tables,
		Indexes: indexes,
	}
}

func Run(engine *Engine, text string) {
	log.Debug("EXEC: ", text)
	result := Parse(strings.TrimSpace(text))

	if result != nil {
		ExecuteStatement(engine, result)
	}
}

func ExecuteStatement(engine *Engine, statement Statement) {
	switch s := (statement).(type) {
	case *CreateTableStatement:
		if _, err := createTable(engine, s); err != nil {
			fmt.Println(err)
		}
	case *InsertStatement:
		if _, err := doInsert(engine, s); err != nil {
			fmt.Println(err)
		}
	case *SelectStatement:
		startingTime := time.Now().UTC()
		i := 0

		if result, err := doSelect(engine, s); err != nil {
			fmt.Println(err)
		} else {
			for row := range result.Rows {
				fmt.Println(row)
				i++
			}
		}

		duration := time.Now().UTC().Sub(startingTime)

		fmt.Printf("\n%d rows (%s)\n", i, duration)
	}
}

func (f *indexedField) Less(than Item) bool {
	return f.value < than.(*indexedField).value
}

func newTableScanner(tableName string) (*csv.Reader, error) {
	csvFile, err := os.Open(filepath.Join("./tsql_data/", tableName, "/data.csv"))

	if err != nil {
		return nil, err
	}

	tableCsv := csv.NewReader(bufio.NewReader(csvFile))

	return tableCsv, nil
}

func buildIndex(job *pkJob) {
	btree := New(5)

	csvReader, err := newTableScanner(job.table.Name)

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

func buildIndexes(m map[string]*TableMetadata) map[string]*BTree {
	indexes := make(map[string]*BTree)
	results := make(chan *pkJob)

	var wg sync.WaitGroup

	for _, t := range m {
		for i, c := range t.Columns {
			if c.PrimaryKey {
				wg.Add(1)
				go func(i int, t *TableMetadata) {
					defer wg.Done()
					job := &pkJob{fieldIndex: i, table: t}
					buildIndex(job)
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

func loadTables() map[string]*TableMetadata {
	tableMetadata := make(map[string]*TableMetadata)

	filepath.Walk("./tsql_data", func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(p, "metadata.json") {
			data, err := ioutil.ReadFile(p)

			if err != nil {
				panic("unable to load tables")
			}

			var metadata TableMetadata
			err = json.Unmarshal(data, &metadata)

			if err != nil {
				panic("unable to load tables")
			}

			tableMetadata[metadata.Name] = &metadata
		}

		return nil
	})

	return tableMetadata
}

func getExecutionEnvironment(engine *Engine, tables []TableAlias) (*ExecutionEnvironment, error) {
	columnLookup := make(map[string]*ColumnReference)
	tableMetadata := make(map[string]*TableMetadata)
	allMetadata := make([]*TableMetadata, len(tables))

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
