package engine

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

func Run(engine *Engine, text string) {
	fmt.Printf("Input:\n%s;\n\n", text)

	result := Parse(strings.TrimSpace(text))
	fmt.Printf("Statement:\n%s;\n\n", result)

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

func getTableMetadata(tableName string) (*TableMetadata, error) {
	metadataPath := filepath.Join("./tsql_data/", tableName, "/metadata.json")
	data, err := ioutil.ReadFile(metadataPath)

	if err != nil {
		return nil, err
	}

	var metadata TableMetadata
	err = json.Unmarshal(data, &metadata)

	return &metadata, err
}

type indexedField struct {
	value string
	offset int
}

func (f *indexedField) Less(than Item) bool {
	return f.value < than.(*indexedField).value
}

func buildIndexes(m map[string]*TableMetadata) map[string]*BTree {
	indexes := make(map[string]*BTree)

	for _, t := range m {
		var primaryKeyIndex = -1
		for i, c := range t.Columns {
			if c.PrimaryKey {
				primaryKeyIndex = i
			}
		}

		if primaryKeyIndex >= 0 {
			btree := New(5)

			csvFile, err := os.Open(filepath.Join("./tsql_data/", t.Name, "/data.csv"))

			if err != nil {
				panic("unable to build index")
			}

			tableCsv, _ := csv.NewReader(bufio.NewReader(csvFile)).ReadAll()

			for r, v := range tableCsv {
				btree.Insert(&indexedField{
					value: v[primaryKeyIndex],
					offset: r,
				})
			}

			indexes[t.Name] = btree
		}
	}

	return indexes
}

func loadTables() map[string]*TableMetadata {
	tableMetadata := make(map[string]*TableMetadata)

	filepath.Walk("./tsql_data", func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(p,"metadata.json") {
			tableName := strings.Replace(path.Dir(p), "tsql_data/", "", 1)
			metadata, _ := getTableMetadata(tableName)
			tableMetadata[tableName] = metadata
		}

		return nil
	})

	return tableMetadata
}

func getExecutionEnvironment(engine *Engine, tables map[string]string) (*ExecutionEnvironment, error) {
	columnLookup := make(map[string]int)
	tableMetadata := make(map[string]*TableMetadata)
	allMetadata := make([]*TableMetadata, len(tables))

	i := 0
	for alias, table := range tables {
		metadata, _ := engine.Tables[table]

		for _, c := range metadata.Columns {
			columnLookup[fmt.Sprintf("%s.%s", alias, c.Name)] = i
			i++
		}

		tableMetadata[alias] = metadata
		allMetadata = append(allMetadata, metadata)
	}

	return &ExecutionEnvironment{
		Tables:       tableMetadata,
		ColumnLookup: columnLookup,
		Engine:       engine,
	}, nil
}

type Engine struct {
	Indexes map[string]*BTree
	Tables  map[string]*TableMetadata
}

func Start() *Engine {
	tables := loadTables()
	indexes := buildIndexes(tables)

	return &Engine{
		Tables: tables,
		Indexes: indexes,
	}
}