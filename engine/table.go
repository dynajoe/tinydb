package engine

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	"github.com/joeandaverde/tinydb/internal/metadata"
)

func (e *Engine) loadTables() {
	newTables := loadTableDefinitions(e.Config)
	e.adminLock.Lock()
	e.Tables = newTables
	e.adminLock.Unlock()
}

func loadTableDefinitions(config *Config) map[string]metadata.TableDefinition {
	tableDefinitions := make(map[string]metadata.TableDefinition)
	matches, _ := filepath.Glob(filepath.Join(config.DataDir, "**/metadata.json"))

	for _, p := range matches {
		data, err := ioutil.ReadFile(p)

		if err != nil {
			panic("unable to load tables")
		}

		var tableDefinition metadata.TableDefinition
		err = json.Unmarshal(data, &tableDefinition)

		if err != nil {
			panic("unable to load tables")
		}

		tableDefinitions[tableDefinition.Name] = tableDefinition
	}

	return tableDefinitions
}
