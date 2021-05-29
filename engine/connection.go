package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
	"github.com/joeandaverde/tinydb/tsql"
	"github.com/sirupsen/logrus"
)

type Control byte

type Response byte

type Command struct {
	Control
	Payload []byte
}

var noMoreRowsErr = errors.New("end of result")

const (
	RESPONSE_ERROR           Response = 'E'
	RESPONSE_COMPLETED       Response = 'C' // + insert id and rows affected
	RESPONSE_ROW_DATA        Response = 'D'
	RESPONSE_ROW_DESCRIPTION Response = 'B'
)

const (
	CONTROL_PARSE    Control = 'P'
	CONTROL_DESCRIBE Control = 'D'
	CONTROL_BIND     Control = 'B'
	CONTROL_EXECUTE  Control = 'E'
	CONTROL_QUERY    Control = 'Q'
	CONTROL_NEXT     Control = 'N'
)

func (c Control) String() string {
	switch c {
	case CONTROL_PARSE:
		return "CONTROL_PARSE"
	case CONTROL_EXECUTE:
		return "CONTROL_EXECUTE"
	case CONTROL_QUERY:
		return "CONTROL_QUERY"
	case CONTROL_DESCRIBE:
		return "CONTROL_DESCRIBE"
	case CONTROL_NEXT:
		return "CONTROL_NEXT"
	default:
		return strconv.Itoa(int(c))
	}
}

// StatementID is a 40 character identifier for the prepared statement
// TODO: this doesn't need to be this long but is easy for sha1
type StatementID string

// Connection is a session that can be used to issue related requests
type Connection struct {
	sync.Mutex
	net.Conn

	log   *logrus.Logger
	pager pager.Pager

	preparedCache map[string]*virtualmachine.PreparedStatement

	flags         *virtualmachine.Flags
	program       *virtualmachine.Program
	queryComplete chan struct{}
	pid           int32

	scratch [512]byte
}

// ResultSet is the result of a query; rows are provided asynchronously
type ResultSet struct {
	Columns []string
	ErrorCh <-chan error
}

// Row is a row in a result
type Row struct {
	Data []interface{}
}

// Handle processes a command on a connection. Only one command can be handled at a time per connection.
func (c *Connection) Handle(ctx context.Context, cmd Command) error {
	c.Lock()
	defer c.Unlock()

	c.log.Debugf("handling command: %s payload size: %v", cmd.Control, len(cmd.Payload))

	switch cmd.Control {
	case CONTROL_PARSE:
		n, text := c.readString(cmd.Payload)
		_, name := c.readString(cmd.Payload[n:])

		c.log.Debugf("preparing: %s @ %s", name, text)
		stmt, err := c.prepare(text)
		if err != nil {
			if err := c.writeByte(RESPONSE_ERROR); err != nil {
				return err
			}
			return nil
		}

		// cache for subsequent execution
		c.preparedCache[name] = stmt

		if err := c.writeByte(RESPONSE_COMPLETED); err != nil {
			return err
		}
		return nil

	case CONTROL_EXECUTE:
		_, name := c.readString(cmd.Payload)
		stmt, ok := c.preparedCache[name]
		if !ok {
			return fmt.Errorf("prepared statement not found")
		}

		c.log.Debugf("executing prepared statement: %s", name)
		if err := c.exec(ctx, stmt); err != nil {
			return fmt.Errorf("error executing statement: %w", err)
		}

		if stmt.Statement.ReturnsRows() {
			c.log.Debug("row description")
			if err := c.writeByte(RESPONSE_ROW_DESCRIPTION); err != nil {
				return err
			}
			if err := c.writeStringColumns(stmt.Columns); err != nil {
				return err
			}

			return nil
		}

		// Not returning rows, wait for in progress query to complete
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.queryComplete:
		}

		c.log.Debug("complete")
		if err := c.writeByte(RESPONSE_COMPLETED); err != nil {
			return err
		}
		return nil

	case CONTROL_QUERY:
		_, commandText := c.readString(cmd.Payload)

		c.log.Debugf("preparing raw query: %s", commandText)
		stmt, err := c.prepare(commandText)
		if err != nil {
			return err
		}

		c.log.Debug("executing raw statement")
		if err := c.exec(ctx, stmt); err != nil {
			return fmt.Errorf("error executing statement: %w", err)
		}

		if stmt.Statement.ReturnsRows() {
			c.log.Debug("row description")
			if err := c.writeByte(RESPONSE_ROW_DESCRIPTION); err != nil {
				return err
			}
			if err := c.writeStringColumns(stmt.Columns); err != nil {
				return err
			}

			return nil
		}

		// Not returning rows, wait for in progress query to complete
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.queryComplete:
		}

		c.log.Debug("complete")
		if err := c.writeByte(RESPONSE_COMPLETED); err != nil {
			return err
		}
		return nil

	case CONTROL_NEXT:
		data, err := c.next(ctx)
		if err != nil {
			if err == noMoreRowsErr {
				c.log.Debug("no more rows")
				return c.writeByte(RESPONSE_COMPLETED)
			}
			return fmt.Errorf("error getting next: %w", err)
		}

		c.log.Debug("writing row data")
		if err := c.writeByte(RESPONSE_ROW_DATA); err != nil {
			return err
		}
		if err := c.writeColumns(data); err != nil {
			return err
		}
		return nil

	default:
		return fmt.Errorf("unknown control character: %d", cmd.Control)
	}
}

func (c *Connection) writeStringColumns(data []string) error {
	// write out number of columns to come
	if err := c.writeUint32(uint32(len(data))); err != nil {
		return err
	}

	for _, v := range data {
		if err := c.writeString(v); err != nil {
			return err
		}
	}

	return nil
}

func (c *Connection) writeColumns(data []interface{}) error {
	// write out number of columns to come
	if err := c.writeUint32(uint32(len(data))); err != nil {
		return err
	}

	for _, d := range data {
		switch v := d.(type) {
		case string:
			if err := c.writeString(v); err != nil {
				return err
			}
		default:
			return errors.New("error getting next: unsupported type")
		}
	}

	return nil
}

func (c *Connection) readString(data []byte) (int, string) {
	textLen := binary.BigEndian.Uint32(data[:4])
	text := string(data[4:][:textLen])
	return int(textLen + 4), text
}

func (c *Connection) writeString(s string) error {
	if err := c.writeUint32(uint32(len(s))); err != nil {
		return err
	}

	if _, err := c.Write([]byte(s)); err != nil {
		return err
	}

	return nil
}

func (c *Connection) writeUint32(n uint32) error {
	binary.BigEndian.PutUint32(c.scratch[:], n)
	_, err := c.Write(c.scratch[:4])
	return err
}
func (c *Connection) writeByte(b Response) error {
	c.scratch[0] = byte(b)
	_, err := c.Write(c.scratch[:1])
	return err
}

// prepare parses and builds a virtual machine program
func (c *Connection) prepare(command string) (*virtualmachine.PreparedStatement, error) {
	stmt, err := tsql.Parse(command)
	if err != nil {
		return nil, err
	}

	// prepare the program
	preparedStmt, err := virtualmachine.Prepare(stmt, c.pager)
	if err != nil {
		return nil, err
	}

	return preparedStmt, nil
}

// exec executes a prepared statement on the database connection
func (c *Connection) exec(ctx context.Context, stmt *virtualmachine.PreparedStatement) error {
	if c.program != nil {
		return fmt.Errorf("only one program can be running at a time")
	}

	c.program = virtualmachine.NewProgram(c.flags, c.pager, stmt)
	c.queryComplete = make(chan struct{})

	go func() {
		defer close(c.queryComplete)

		pid := atomic.AddInt32(&c.pid, 1)
		log := c.log.WithField("pid", pid).Logger

		defer log.Debugf("program complete")

		log.Debugf("program starting")
		err := c.program.Run(ctx)
		log.WithError(err).Debugf("program exited")

		if err := c.complete(err); err != nil {
			log.WithError(err).Errorf("error running program")
			return
		}
	}()

	return nil
}

// next returns the next result from the current running program or an error
// indicating that the result is complete.
func (c *Connection) next(ctx context.Context) ([]interface{}, error) {
	if c.program == nil {
		// there's no program running to provide results
		return nil, noMoreRowsErr
	}

	// TODO: it's possible that the results don't matter and the
	// transaction should still be committed, at present
	// this implementation requires reading all output before commit
	result, ok := <-c.program.Output()

	// no more results, complete the statement
	if !ok {
		return nil, noMoreRowsErr
	}

	return result.Data, nil
}

// complete ensures a program exits cleanly and leaves the connection in a deterministic state
func (c *Connection) complete(runError error) error {
	// always discard program
	defer func() {
		c.program = nil
	}()

	if runError != nil || c.flags.Rollback {
		c.log.Debug("complete: rollback")
		c.rollback()
		return runError
	}

	if c.flags.AutoCommit {
		c.log.Debug("complete: commit")
		if err := c.pager.Flush(); err != nil {
			c.log.WithError(err).Error("complete: error committing")
			c.rollback()
			return err
		}
	}

	return nil
}

// rollback rolls back any changes made during the program execution
func (c *Connection) rollback() {
	c.flags.AutoCommit = true
	c.flags.Rollback = false
	c.pager.Reset()
}
