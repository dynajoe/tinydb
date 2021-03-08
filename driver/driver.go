package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/joeandaverde/tinydb/engine"
)

func init() {
	sql.Register("tinydb", &TinyDBDriver{})
}

type TinyDBDriver struct{}

type TinyDBConnection struct {
	dsn  string
	conn *engine.Connection
}

type TinyDBStmt struct {
	command string
	conn    *TinyDBConnection
}

type TinyDBTx struct {
	conn *TinyDBConnection
}

type TinyDBResult struct{}

type TinyDBRows struct {
	rs *engine.ResultSet
}

// Open opens a tinydb connection
func (c *TinyDBDriver) Open(dsn string) (driver.Conn, error) {
	config, err := parseDsn(dsn)
	if err != nil {
		return nil, err
	}

	// TODO: Support more than 1 connection and
	engine, err := engine.Start(config)
	if err != nil {
		return nil, err
	}

	conn := engine.Connect()

	return &TinyDBConnection{
		dsn:  dsn,
		conn: conn,
	}, nil
}

// Prepare prepares a tinydb query
func (c *TinyDBConnection) Prepare(command string) (driver.Stmt, error) {
	return &TinyDBStmt{
		command: command,
		conn:    c,
	}, nil
}

// Begin begins a tinydb transaction
func (c *TinyDBConnection) Begin() (driver.Tx, error) {
	_, err := c.exec("BEGIN")
	if err != nil {
		return nil, err
	}
	return &TinyDBTx{c}, nil
}

// Close closes a tinydb connection
func (c *TinyDBConnection) Close() error {
	return nil
}

func (c *TinyDBConnection) exec(command string) (driver.Result, error) {
	rs, err := c.conn.Exec(command)
	if err != nil {
		return nil, err
	}

	// Wait for exec to finish, discard any rows returned
	for r := range rs.Results {
		if r.Error != nil {
			return nil, r.Error
		}
	}

	return &TinyDBResult{}, nil
}

func (c *TinyDBConnection) query(command string) (driver.Rows, error) {
	rs, err := c.conn.Exec(command)
	if err != nil {
		return nil, err
	}

	return &TinyDBRows{
		rs: rs,
	}, nil
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
// Close closes a tinydb connection
func (c *TinyDBStmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (c *TinyDBStmt) NumInput() int {
	return -1
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (c *TinyDBStmt) Exec(args []driver.Value) (driver.Result, error) {
	_, err := c.conn.exec(c.command)
	if err != nil {
		return nil, err
	}

	return &TinyDBResult{}, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
func (c *TinyDBStmt) Query(args []driver.Value) (driver.Rows, error) {
	return c.conn.query(c.command)
}

func (t *TinyDBTx) Commit() error {
	_, err := t.conn.exec("COMMIT")
	return err
}

func (t *TinyDBTx) Rollback() error {
	_, err := t.conn.exec("ROLLBACK")
	return err
}

func (r *TinyDBResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *TinyDBResult) RowsAffected() (int64, error) {
	return 0, nil
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *TinyDBRows) Columns() []string {
	return r.rs.Columns[:]
}

// Close closes the rows iterator.
func (r *TinyDBRows) Close() error {
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *TinyDBRows) Next(dest []driver.Value) error {
	row := <-r.rs.Results
	if row == nil {
		return io.EOF
	} else if row.Error != nil {
		return row.Error
	}

	for i, v := range row.Data {
		dest[i] = v
	}
	return nil
}

func parseDsn(dsn string) (*engine.Config, error) {
	pageSize := 4096
	dbPath := dsn

	pos := strings.IndexRune(dsn, '?')
	if pos >= 1 {
		dbPath = dsn[:pos]
		params, err := url.ParseQuery(dsn[pos+1:])
		if err != nil {
			return nil, err
		}

		if val := params.Get("page_size"); val != "" {
			iv, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid page_size: %v: %v", val, err)
			}
			pageSize = int(iv)
		}

	}

	return &engine.Config{
		DataDir:  dbPath,
		PageSize: pageSize,
	}, nil
}

var _ driver.Driver = (*TinyDBDriver)(nil)

var _ driver.Conn = (*TinyDBConnection)(nil)

var _ driver.Stmt = (*TinyDBStmt)(nil)

var _ driver.Tx = (*TinyDBTx)(nil)

var _ driver.Result = (*TinyDBResult)(nil)

var _ driver.Rows = (*TinyDBRows)(nil)
