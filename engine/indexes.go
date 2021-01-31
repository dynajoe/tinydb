package engine

import (
	"sync"

	"github.com/joeandaverde/tinydb/internal/btree"
)

type pkJob struct {
	table      TableDefinition
	fieldIndex int
	result     *btree.BTree
}

type indexedField struct {
	value   string
	offsets []int64
}

func (f *indexedField) Less(than btree.Item) bool {
	return f.value < than.(*indexedField).value
}

func (e *Engine) loadIndexes() {
	newIndexes := buildIndexes(e.Config, e.Tables)
	e.adminLock.Lock()
	e.Indexes = newIndexes
	e.adminLock.Unlock()
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
