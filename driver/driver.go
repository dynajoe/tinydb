package driver

import (
	"bytes"
	"crypto/sha1"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/joeandaverde/tinydb/engine"
)

func init() {
	sql.Register("tinydb", &TinyDBDriver{})
}

type TinyDBDriver struct {
	testDialer func() (net.Conn, error)
}

type TinyDBConnection struct {
	dsn  string
	conn net.Conn

	scratch [512]byte
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

// Prepare prepares a query
func (c *TinyDBConnection) Prepare(text string) (driver.Stmt, error) {
	commandHash := sha1.Sum([]byte(text))
	statementID := fmt.Sprintf("%x", commandHash[:])

	// prepare payload: <uint32:len sql><utf-8:sql><uint32:len name><utf-8:name>
	buf := bytes.Buffer{}

	// sql
	binary.BigEndian.PutUint32(c.scratch[:], uint32(len(text)))
	io.Copy(&buf, bytes.NewReader(c.scratch[0:4]))
	buf.WriteString(text)

	// name
	binary.BigEndian.PutUint32(c.scratch[:], uint32(len(statementID)))
	io.Copy(&buf, bytes.NewReader(c.scratch[0:4]))
	buf.WriteString(statementID)

	// prepare the query
	if err := c.sendCommand(engine.CONTROL_PARSE, buf.Bytes()); err != nil {
		return nil, err
	}

	// read response
	res, err := c.readByte()
	if err != nil {
		return nil, err
	}

	switch engine.Response(res) {
	case engine.RESPONSE_COMPLETED:
		return &TinyDBStmt{
			id:      statementID,
			command: text,
			conn:    c,
		}, nil
	case engine.RESPONSE_ERROR:
		return nil, fmt.Errorf("prepare error")
	default:
		return nil, fmt.Errorf("unexpected prepare query response")
	}
}

// Begin begins a transaction
func (c *TinyDBConnection) Begin() (driver.Tx, error) {
	if _, err := c.simpleQuery("BEGIN"); err != nil {
		return nil, err
	}

	return &TinyDBTx{c}, nil
}

// Close closes a connection
func (c *TinyDBConnection) Close() error {
	return c.conn.Close()
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
	return int64(r.rowsAffected), nil
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
	if err := r.conn.sendCommand(engine.CONTROL_NEXT, nil); err != nil {
		return fmt.Errorf("error sending next command: %w", err)
	}

	res, err := r.conn.readByte()
	if err != nil {
		return err
	}

	switch engine.Response(res) {
	case engine.RESPONSE_ROW_DATA:
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

	case engine.RESPONSE_COMPLETED:
		return io.EOF
	case engine.RESPONSE_ERROR:
		return fmt.Errorf("query error")
	default:
		return fmt.Errorf("unexpected response: %v", engine.Response(res))
	}
}

func (c *TinyDBConnection) execNonQuery(id string) (int64, error) {
	if err := c.sendCommand(engine.CONTROL_EXECUTE, packString(id)); err != nil {
		return 0, err
	}
	return c.readNonQueryResponse()
}

func (c *TinyDBConnection) execQuery(id string) ([]string, error) {
	if err := c.sendCommand(engine.CONTROL_EXECUTE, packString(id)); err != nil {
		return nil, err
	}
	return c.readQueryResponse()
}

func (c *TinyDBConnection) simpleQuery(query string) ([]string, error) {
	if err := c.sendCommand(engine.CONTROL_QUERY, packString(query)); err != nil {
		return nil, err
	}
	return c.readQueryResponse()
}

func (c *TinyDBConnection) sendCommand(ctrl engine.Control, payload []byte) error {
	c.scratch[0] = byte(ctrl)
	binary.BigEndian.PutUint32(c.scratch[1:], uint32(len(payload)))

	// let the server know what to expect
	if _, err := c.conn.Write(c.scratch[:5]); err != nil {
		return err
	}

	// write the data
	if _, err := c.conn.Write(payload); err != nil {
		return err
	}
	return nil
}

func (c *TinyDBConnection) readNonQueryResponse() (int64, error) {
	res, err := c.readByte()
	if err != nil {
		return 0, err
	}

	switch engine.Response(res) {
	case engine.RESPONSE_COMPLETED:
		return 0, nil

	case engine.RESPONSE_ERROR:
		return 0, fmt.Errorf("error executing query")

	default:
		return 0, fmt.Errorf("unexpected response")
	}
}

func (c *TinyDBConnection) readQueryResponse() ([]string, error) {
	res, err := c.readByte()
	if err != nil {
		return nil, err
	}

	switch engine.Response(res) {
	case engine.RESPONSE_COMPLETED:
		return nil, nil

	case engine.RESPONSE_ERROR:
		return nil, fmt.Errorf("error executing query")

	case engine.RESPONSE_ROW_DESCRIPTION:
		row, err := c.readRow()
		if err != nil {
			return nil, err
		}

		cols := make([]string, 0, len(row))
		for _, c := range row {
			cols = append(cols, string(c.([]byte)))
		}

		return cols, nil
	default:
		return nil, fmt.Errorf("unexpected response")
	}
}

func (c *TinyDBConnection) readByte() (byte, error) {
	buf := [1]byte{}
	_, err := io.ReadFull(c.conn, buf[:])
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (c *TinyDBConnection) readUint32() (uint32, error) {
	// 4 bytes for the number of columns to read
	if _, err := io.ReadFull(c.conn, c.scratch[:4]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(c.scratch[:4]), nil
}

func (c *TinyDBConnection) readRow() ([]interface{}, error) {
	columnCount, err := c.readUint32()
	if err != nil {
		return nil, fmt.Errorf("error reading column count from server: %w", err)
	}

	dest := make([]interface{}, columnCount)
	for i := 0; i < int(columnCount); i++ {
		columnLen, err := c.readUint32()
		if err != nil {
			return nil, fmt.Errorf("error reading column length from server: %w", err)
		}
		if columnLen > 1024 {
			return nil, fmt.Errorf("column data too big: %d", columnLen)
		}

		columnData := make([]byte, columnLen)
		if _, err := io.ReadFull(c.conn, columnData); err != nil {
			return nil, fmt.Errorf("error reading column data from server: %w", err)
		}

		dest[i] = columnData
	}

	return dest, nil
}

func packString(s string) []byte {
	packed := make([]byte, 4, len(s)+4)
	binary.BigEndian.PutUint32(packed[:], uint32(len(s)))
	packed = append(packed, []byte(s)...)
	return packed
}

var _ driver.Driver = (*TinyDBDriver)(nil)

var _ driver.Conn = (*TinyDBConnection)(nil)

var _ driver.Stmt = (*TinyDBStmt)(nil)

var _ driver.Tx = (*TinyDBTx)(nil)

var _ driver.Result = (*TinyDBResult)(nil)

var _ driver.Rows = (*TinyDBRows)(nil)
