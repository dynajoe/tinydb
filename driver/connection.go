package driver

import (
	"bytes"
	"crypto/sha1"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"github.com/joeandaverde/tinydb/internal/server"
	"io"
	"net"
)

type TinyDBConnection struct {
	dsn  string
	conn net.Conn

	scratch [512]byte
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
	if err := c.sendCommand(server.ControlParse, buf.Bytes()); err != nil {
		return nil, err
	}

	// read response
	res, err := c.readByte()
	if err != nil {
		return nil, err
	}

	switch server.Response(res) {
	case server.ResponseCompleted:
		return &TinyDBStmt{
			id:      statementID,
			command: text,
			conn:    c,
		}, nil
	case server.ResponseError:
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

func (c *TinyDBConnection) execNonQuery(id string) (int64, error) {
	if err := c.sendCommand(server.ControlExecute, packString(id)); err != nil {
		return 0, err
	}
	return c.readNonQueryResponse()
}

func (c *TinyDBConnection) execQuery(id string) ([]string, error) {
	if err := c.sendCommand(server.ControlExecute, packString(id)); err != nil {
		return nil, err
	}
	return c.readQueryResponse()
}

func (c *TinyDBConnection) simpleQuery(query string) ([]string, error) {
	if err := c.sendCommand(server.ControlQuery, packString(query)); err != nil {
		return nil, err
	}
	return c.readQueryResponse()
}

func (c *TinyDBConnection) sendCommand(ctrl server.Control, payload []byte) error {
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

	switch server.Response(res) {
	case server.ResponseCompleted:
		return 0, nil

	case server.ResponseError:
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

	switch server.Response(res) {
	case server.ResponseCompleted:
		return nil, nil

	case server.ResponseError:
		return nil, fmt.Errorf("error executing query")

	case server.ResponseRowDescription:
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

var _ driver.Conn = (*TinyDBConnection)(nil)
