package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	backend2 "github.com/joeandaverde/tinydb/internal/backend"
	"net"
	"strconv"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
)

type (
	Control byte

	Response byte

	Command struct {
		Control
		Payload []byte
	}
)

const (
	ResponseError          Response = 'E'
	ResponseCompleted      Response = 'C'
	ResponseRowData        Response = 'D'
	ResponseRowDescription Response = 'B'
)

const (
	ControlParse    Control = 'P'
	ControlDescribe Control = 'D'
	ControlBind     Control = 'B'
	ControlExecute  Control = 'E'
	ControlQuery    Control = 'Q'
	ControlNext     Control = 'N'
)

var errNoMoreRows = errors.New("end of result")

func (c Control) String() string {
	switch c {
	case ControlParse:
		return "CONTROL_PARSE"
	case ControlExecute:
		return "CONTROL_EXECUTE"
	case ControlQuery:
		return "CONTROL_QUERY"
	case ControlDescribe:
		return "CONTROL_DESCRIBE"
	case ControlNext:
		return "CONTROL_NEXT"
	default:
		return strconv.Itoa(int(c))
	}
}

// Connection is a session that can be used to issue related requests
type Connection struct {
	sync.Mutex
	net.Conn

	log           *logrus.Logger
	pager         pager.Pager
	backend       *backend2.Backend
	preparedCache map[string]*virtualmachine.PreparedStatement
	proc          *backend2.ProgramInstance

	scratch [512]byte
}

func NewConnection(logger *logrus.Logger, p pager.Pager, conn net.Conn) *Connection {
	return &Connection{
		Conn:          conn,
		log:           logger,
		pager:         p,
		preparedCache: make(map[string]*virtualmachine.PreparedStatement),
		backend:       backend2.NewBackend(logger, p),
	}
}

// Handle processes a command on a connection. Only one command can be handled at a time per connection.
func (c *Connection) Handle(ctx context.Context, cmd Command) error {
	c.Lock()
	defer c.Unlock()

	c.log.Debugf("handling command: %s payload size: %v", cmd.Control, len(cmd.Payload))

	switch cmd.Control {
	case ControlParse:
		n, text := c.readString(cmd.Payload)
		_, name := c.readString(cmd.Payload[n:])

		c.log.Debugf("preparing: %s @ %s", name, text)
		stmt, err := c.backend.Prepare(text)
		if err != nil {
			if err := c.writeByte(ResponseError); err != nil {
				return err
			}
			return nil
		}

		// cache for subsequent execution
		c.preparedCache[name] = stmt

		if err := c.writeByte(ResponseCompleted); err != nil {
			return err
		}
		return nil

	case ControlExecute:
		_, name := c.readString(cmd.Payload)
		stmt, ok := c.preparedCache[name]
		if !ok {
			return fmt.Errorf("prepared statement not found")
		}

		return c.exec(ctx, name, stmt)

	case ControlQuery:
		_, commandText := c.readString(cmd.Payload)

		stmt, err := c.backend.Prepare(commandText)
		if err != nil {
			return err
		}

		return c.exec(ctx, "(unnamed)", stmt)

	case ControlNext:
		if c.proc == nil {
			return errors.New("unexpected next when no statement is executing")
		}

		data, err := c.next(ctx, c.proc)
		if err != nil {
			if err == errNoMoreRows {
				c.log.Debug("no more rows")
				return c.writeByte(ResponseCompleted)
			}
			return fmt.Errorf("error getting next: %w", err)
		}

		c.log.Debug("writing row data")
		if err := c.writeByte(ResponseRowData); err != nil {
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

func (c *Connection) exec(ctx context.Context, name string, stmt *virtualmachine.PreparedStatement) error {
	c.log.Debugf("statement: %s", name)

	proc, err := c.backend.Exec(ctx, stmt)
	if err != nil {
		return fmt.Errorf("error executing statement: %w", err)
	}
	c.proc = proc

	if stmt.Statement.ReturnsRows() {
		if err := c.writeByte(ResponseRowDescription); err != nil {
			return err
		}
		if err := c.writeStringColumns(stmt.Columns); err != nil {
			return err
		}

		return nil
	}

	defer func() { c.proc = nil }()

	// Not returning rows, wait for query to complete
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-proc.Exit:
	}

	if err := c.writeByte(ResponseCompleted); err != nil {
		return err
	}
	return nil
}

// next returns the next result from the program instance or an error
// indicating that the result is complete.
func (c *Connection) next(ctx context.Context, p *backend2.ProgramInstance) ([]interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result, ok := <-p.Output:
		if !ok {
			return nil, errNoMoreRows
		}
		return result.Data, nil
	}
}

func (c *Connection) readString(data []byte) (int, string) {
	textLen := binary.BigEndian.Uint32(data[:4])
	text := string(data[4:][:textLen])
	return int(textLen + 4), text

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
