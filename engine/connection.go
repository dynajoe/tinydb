package engine

import (
	"sync"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
	"github.com/joeandaverde/tinydb/tsql"
)

// Connection is a session that can be used to issue related requests
type Connection struct {
	mu *sync.Mutex

	id         int
	autoCommit bool
	flags      *virtualmachine.Flags
	engine     *Engine
}

// ColumnList represents a list of columns of a result set
type ColumnList []string

// ResultSet is the result of a query; rows are provided asynchronously
type ResultSet struct {
	Columns ColumnList
	Results <-chan *Row
}

// Row is a row in a result
type Row struct {
	Data  []interface{}
	Error error
}

// Exec executes a command on the database connection
func (c *Connection) Exec(command string) (*ResultSet, error) {
	stmt, err := tsql.Parse(command)
	if err != nil {
		return nil, err
	}

	// Only one command can be executing at a time on a connection
	c.mu.Lock()

	// Get a pager in read or write mode
	mode := pager.ModeRead
	if stmt.Mutates() {
		mode = pager.ModeWrite
	}
	pager, err := c.engine.GetPager(c, mode)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	// Prepare the program
	preparedStmt, err := virtualmachine.Prepare(stmt, pager)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	program := virtualmachine.NewProgram(c.flags, pager, preparedStmt)

	rowChan := make(chan *Row)

	program.Run()
	go func() {
		defer close(rowChan)
		defer c.mu.Unlock()

		var err error
		for r := range program.Results() {
			if r.Error != nil {
				err = r.Error
				break
			}

			rowChan <- &Row{
				Data:  r.Data,
				Error: r.Error,
			}
		}

		forceRollback := err != nil

		if forceRollback {
			c.flags.AutoCommit = true
			c.flags.Rollback = false
			pager.Reset()
		}

		// update auto commit flag
		c.autoCommit = c.flags.AutoCommit

		if c.autoCommit {
			if c.flags.Rollback {
				pager.Reset()
				c.flags.Rollback = false
			} else {
				if err := pager.Flush(); err != nil {
					rowChan <- &Row{
						Error: err,
					}
				}
			}
		}

		// AutoCommit mode doesn't need to hold on to a pager.
		if c.flags.AutoCommit {
			c.engine.ReturnPager(c)
		}
	}()

	return &ResultSet{
		Columns: preparedStmt.Columns,
		Results: rowChan,
	}, nil
}
