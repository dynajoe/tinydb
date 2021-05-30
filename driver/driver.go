package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/joeandaverde/tinydb/internal/server"
	"io"
	"net"
)

func init() {
	sql.Register("tinydb", &TinyDBDriver{})
}

type TinyDBDriver struct {
	testDialer func() (net.Conn, error)
}

type TinyDBStmt struct {
	id      string
	command string
	conn    *TinyDBConnection
}

type TinyDBTx struct {
	conn *TinyDBConnection
}

type TinyDBResult struct {
	rowsAffected int64
}

type TinyDBRows struct {
	conn    *TinyDBConnection
	columns []string
}

// Open opens a tinydb connection
func (c *TinyDBDriver) Open(dsn string) (driver.Conn, error) {
	var conn net.Conn
	var err error
	if c.testDialer != nil {
		conn, err = c.testDialer()
	} else {
		conn, err = net.Dial("tcp", dsn)
	}

	if err != nil {
		return nil, err
	}

	return &TinyDBConnection{
		dsn:  dsn,
		conn: conn,
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
func (c *TinyDBStmt) Exec(args []driver.Value) (driver.Result, error) {
	// TODO: bind parameters

	// execute query that doesn't expect results
	rowsAffected, err := c.conn.execNonQuery(c.id)
	if err != nil {
		return nil, fmt.Errorf("error executing non-query prepared statement: %w", err)
	}

	return &TinyDBResult{
		rowsAffected: rowsAffected,
	}, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
func (c *TinyDBStmt) Query(args []driver.Value) (driver.Rows, error) {
	// TODO: bind parameters

	// execute the prepared statement
	cols, err := c.conn.execQuery(c.id)
	if err != nil {
		return nil, fmt.Errorf("error executing prepared statement: %w", err)
	}

	return &TinyDBRows{conn: c.conn, columns: cols}, nil
}

func (t *TinyDBTx) Commit() error {
	if _, err := t.conn.simpleQuery("COMMIT"); err != nil {
		return err
	}
	return nil
}

func (t *TinyDBTx) Rollback() error {
	if _, err := t.conn.simpleQuery("ROLLBACK"); err != nil {
		return err
	}
	return nil
}

func (r *TinyDBResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *TinyDBResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *TinyDBRows) Columns() []string {
	return r.columns
}

// Close closes the rows iterator.
func (r *TinyDBRows) Close() error {
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next returns io.EOF when there are no more rows.
func (r *TinyDBRows) Next(dest []driver.Value) error {
	if err := r.conn.sendCommand(server.ControlNext, nil); err != nil {
		return fmt.Errorf("error sending next command: %w", err)
	}

	res, err := r.conn.readByte()
	if err != nil {
		return err
	}

	switch server.Response(res) {
	case server.ResponseRowData:
		data, err := r.conn.readRow()
		if err != nil {
			return fmt.Errorf("error reading row data: %w", err)
		}

		if len(data) != len(dest) {
			return fmt.Errorf("unexpected column count from server got %d expected %d", len(data), len(dest))
		}

		for i, c := range data {
			dest[i] = c
		}
		return nil

	case server.ResponseCompleted:
		return io.EOF
	case server.ResponseError:
		return fmt.Errorf("query error")
	default:
		return fmt.Errorf("unexpected response: %v", server.Response(res))
	}
}

var _ driver.Driver = (*TinyDBDriver)(nil)

var _ driver.Stmt = (*TinyDBStmt)(nil)

var _ driver.Tx = (*TinyDBTx)(nil)

var _ driver.Result = (*TinyDBResult)(nil)

var _ driver.Rows = (*TinyDBRows)(nil)
