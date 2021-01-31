package engine

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	"github.com/joeandaverde/tinydb/internal/storage"
)

// ColumnDefinition represents a specification for a column in a table
type ColumnDefinition struct {
	Name       string          `json:"name"`
	Type       storage.SQLType `json:"type"`
	Offset     int             `json:"offset"`
	PrimaryKey bool            `json:"is_primary_key"`
}

type TableDefinition struct {
	Name     string             `json:"name"`
	RawText  string             `json:"raw_text"`
	Columns  []ColumnDefinition `json:"columns"`
	RootPage int                `json:"root_page"`
}

// Row is a row in a result
type Row struct {
	Data    []interface{}
	Offset  int64
	IsValid bool
}

type RowReader interface {
	Scan() bool
	Read() Row
}

// TODO: this is to get things to compile, need to actually get auto incr key
var keys = make(map[string]int)

func NextKey(tableName string) int {
	if _, ok := keys[tableName]; !ok {
		keys[tableName] = 0
	}
	keys[tableName] = keys[tableName] + 1
	return keys[tableName]
}

func (e *Engine) loadTables() {
	newTables := loadTableDefinitions(e.Config)
	e.adminLock.Lock()
	e.Tables = newTables
	e.adminLock.Unlock()
}

// CreateTable creates a new database table
func (e *Engine) CreateTable(table *TableDefinition) error {
	pageOne, err := e.Pager.Read(1)
	if err != nil {
		return err
	}

	// TODO: the recordKey should be an int from an auto index perhaps?
	recordKey := NextKey("master")

	// Allocate a page for the new table
	rootPage, err := e.Pager.Allocate()
	if err != nil {
		return err
	}

	// Update Page 1 with the new table record
	tableRecord := storage.NewMasterTableRecord("table", table.Name,
		table.Name, rootPage.PageNumber, table.RawText)

	if err := storage.WriteRecord(pageOne, recordKey, tableRecord); err != nil {
		return err
	}
	if err := e.Pager.Write(pageOne, rootPage); err != nil {
		return err
	}

	return nil
}

// InsertRecord inserts a record into the provided table
func (e *Engine) InsertRecord(tableName string, fields []*storage.Field) error {
	metadata, err := e.GetTableDefinition(tableName)
	if err != nil {
		return err
	}

	rootPage, err := e.Pager.Read(metadata.RootPage)
	if err != nil {
		return err
	}

	rowID := NextKey(tableName)
	record := storage.NewRecord(fields)
	if err := storage.WriteRecord(rootPage, rowID, record); err != nil {
		return err
	}
	if err := e.Pager.Write(rootPage); err != nil {
		return err
	}

	return nil
}

func (c *ColumnDefinition) DefaultValue() interface{} {
	if c.PrimaryKey {
		return NextKey(c.Name)
	}
	return c.DefaultValue
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
