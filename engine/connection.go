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

	id            int
	autoCommit    bool
	flags         *virtualmachine.Flags
	engine        *Engine
	reservedPager *pager.ReservedPager
}

// ColumnList represents a list of columns of a result set
type ColumnList []string

// ResultSet is the result of a query; rows are provided asynchronously
type ResultSet struct {
	Columns ColumnList
	Rows    <-chan *Row
	Error   <-chan error
}

// Row is a row in a result
type Row struct {
	Data []interface{}
}

// Exec executes a command on the database connection
func (c *Connection) Exec(text string) (*ResultSet, error) {
	stmt, err := tsql.Parse(text)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()

	// If there's no pager reserved, get one.
	if c.reservedPager == nil {
		c.reservedPager = c.engine.pager.Reserve(pager.ModeRead)
	}
	pager := c.reservedPager.Pager()

	preparedStmt, err := virtualmachine.Prepare(stmt, pager)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	program := virtualmachine.NewProgram(c.flags, pager, preparedStmt)

	// If this query mutates, upgrade the pager to a writer.
	if stmt.Mutates() {
		// This will block until all readers and writers are finished.
		c.reservedPager.Upgrade()
	}

	rowChan := make(chan *Row)
	errChan := make(chan error, 1)

	go func() {
		defer c.mu.Unlock()

		if err := program.Run(); err != nil {
			// on error force rollback of any in progress transactions, ignore errors.
			_ = c.handleTx(true)
			errChan <- err
		} else if err := c.handleTx(false); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer close(rowChan)
		for r := range program.Results() {
			rowChan <- &Row{
				Data: r,
			}
		}
	}()

	return &ResultSet{
		Columns: preparedStmt.Columns,
		Rows:    rowChan,
		Error:   errChan,
	}, nil
}

func (c *Connection) handleTx(forceRollback bool) error {
	pager := c.reservedPager.Pager()

	if forceRollback {
		c.flags.AutoCommit = true
		c.flags.Rollback = false
		pager.Reset()
		c.reservedPager.Release()
		c.reservedPager = nil
		return nil
	}

	// update auto commit flag
	c.autoCommit = c.flags.AutoCommit

	if c.autoCommit {
		if c.flags.Rollback {
			pager.Reset()
			c.flags.Rollback = false
		} else {
			if err := pager.Flush(); err != nil {
				return err
			}
		}
		c.reservedPager.Release()
		c.reservedPager = nil
	}

	return nil
}
