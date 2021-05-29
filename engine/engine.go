package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"path"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
)

var ErrServerClosed = errors.New("tinydb: Server closed")

// Config describes the configuration for the database
type Config struct {
	DataDir          string       `yaml:"data_directory"`
	PageSize         int          `yaml:"page_size"`
	Addr             string       `yaml:"listen_address"`
	MaxReceiveBuffer int          `yaml:"max_receive_buffer"`
	LogLevel         logrus.Level `yaml:"log_level"`
}

// Engine holds metadata and indexes about the database
type Engine struct {
	sync.RWMutex
	log       *log.Logger
	config    *Config
	wal       *storage.WAL
	pagerPool *pager.Pool
	txID      uint32
}

// Start initializes a new TinyDb database engine
func Start(config *Config) (*Engine, error) {
	logger := log.New()
	logger.SetLevel(config.LogLevel)

	logger.Infof("Starting database engine [DataDir: %s]", config.DataDir)

	if config.PageSize < 1024 {
		return nil, errors.New("page size must be greater than or equal to 1024")
	}

	dbPath := path.Join(config.DataDir, "tiny.db")

	// Open the main database file
	dbFile, err := storage.OpenDbFile(dbPath, config.PageSize)
	if err != nil {
		return nil, err
	}

	// Brand new database needs at least one page.
	if dbFile.TotalPages() == 0 {
		if err := pager.Initialize(dbFile); err != nil {
			return nil, err
		}
	}

	// Initialize WAL
	wal, err := storage.OpenWAL(dbFile)
	if err != nil {
		return nil, err
	}

	return &Engine{
		config:    config,
		log:       logger,
		wal:       wal,
		pagerPool: pager.NewPool(pager.NewPager(wal)),
	}, nil
}

func Serve(ctx context.Context, ln net.Listener, engine *Engine) error {
	for {
		conn, err := ln.Accept()

		// stop accepting connection on context cancel
		select {
		case <-ctx.Done():
			return ErrServerClosed
		default:
		}

		if err != nil {
			engine.log.WithError(err).Error("error acceping new connection")
			// TODO: prevent mass amounts of errors with backoff and or closing the server completely
		}

		// handle the connection
		go engine.Serve(conn)
	}
}

// Connect establishes a new connection to the database engine
func (e *Engine) Serve(conn net.Conn) {
	defer conn.Close()
	e.log.Infof("connect: %+v", conn.RemoteAddr())

	// setup a new engine connection
	dbConn := &Connection{
		log:           e.log.WithField("remote_addr", conn.RemoteAddr()).Logger,
		Conn:          conn,
		flags:         &virtualmachine.Flags{AutoCommit: true},
		pager:         pager.NewPager(e.wal),
		preparedCache: make(map[string]*virtualmachine.PreparedStatement),
	}

	// 1 byte for control
	// 4 bytes for payload length
	controlHeader := [5]byte{}

	// buffer for handling payloads
	payloadBuffer := make([]byte, 0, e.config.MaxReceiveBuffer)

	// TODO: handle errors gracefully rather than closing connection
	for {
		n, err := dbConn.Read(controlHeader[:])
		if err != nil {
			e.log.Error("error reading control header")
			return
		}

		// not enough bytes read in control header
		if n != len(controlHeader) {
			e.log.Error("invalid control header length")
			return
		}

		// read payload
		payloadLen := binary.BigEndian.Uint32(controlHeader[1:])
		if int(payloadLen) > e.config.MaxReceiveBuffer {
			e.log.Error("invalid payload size")
			return
		}

		if payloadLen > 0 {
			payloadRead, err := conn.Read(payloadBuffer[:payloadLen])
			if err != nil {
				e.log.WithError(err).Error("error reading payload")
				return
			}
			if payloadRead != int(payloadLen) {
				e.log.Errorf("error reading full payload: expected %d got %d", payloadLen, payloadRead)
				return
			}
		}

		// handle the command
		if err := dbConn.Handle(context.Background(), Command{
			Control: Control(controlHeader[0]),
			Payload: payloadBuffer[:payloadLen],
		}); err != nil {
			e.log.WithError(err).Error("error handling command")
			return
		}
	}
}

// TxID provides a new transaction id
func (e *Engine) TxID() uint32 {
	return atomic.AddUint32(&e.txID, 1)
}
