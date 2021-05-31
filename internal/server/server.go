package server

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/joeandaverde/tinydb/internal/backend"
	"github.com/sirupsen/logrus"
	"io"
	"net"
)

var ErrServerClosed = errors.New("tinydb: Server closed")

type Server struct {
	config     Config
	shutdownCh chan struct{}
	log        logrus.FieldLogger
}

type Config struct {
	MaxRecvSize int
}

func NewServer(log logrus.FieldLogger, config Config) *Server {
	return &Server{
		config:     config,
		shutdownCh: make(chan struct{}),
		log:        log,
	}
}

func (s *Server) Serve(ln net.Listener, engine *backend.Engine) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.log.WithError(err).Error("error accepting new connection")
			// TODO: prevent mass amounts of errors with backoff and or closing the server completely
			continue
		}

		// stop accepting connection on shutdown
		select {
		case <-s.shutdownCh:
			return ErrServerClosed
		default:
		}

		// handle the connection
		go s.Handle(conn, engine)
	}
}

func (s *Server) Shutdown() error {
	return nil
}

// Handle handles client connection
func (s *Server) Handle(conn net.Conn, engine *backend.Engine) {
	s.log.Infof("connect: %+v", conn.RemoteAddr())

	dbConn := NewConnection(s.log, engine.NewPager(), conn)
	defer dbConn.Close()

	// TODO: handle errors gracefully rather than closing connection
	for {
		// 1 byte for control
		// 4 bytes for payload length
		_, err := io.ReadFull(dbConn, dbConn.recvBuffer[:5])
		if err != nil {
			s.log.Error("error reading control header")
			return
		}

		// read payload
		control := Control(dbConn.recvBuffer[0])
		payloadLen := binary.BigEndian.Uint32(dbConn.recvBuffer[1:])
		if int(payloadLen) > len(dbConn.recvBuffer) {
			s.log.Error("invalid payload size")
			return
		}

		if payloadLen > 0 {
			_, err := io.ReadFull(dbConn, dbConn.recvBuffer[:payloadLen])
			if err != nil {
				s.log.WithError(err).Error("error reading payload")
				return
			}
		}

		// handle the command
		if err := dbConn.Handle(context.Background(), Command{
			Control: control,
			Payload: dbConn.recvBuffer[:payloadLen],
		}); err != nil {
			s.log.WithError(err).Error("terminating connection: error handling command")
			return
		}
	}
}
