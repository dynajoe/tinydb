package server

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/joeandaverde/tinydb/internal/backend"
	"github.com/sirupsen/logrus"
	"net"
)

var ErrServerClosed = errors.New("tinydb: Server closed")

type Server struct {
	config     Config
	shutdownCh chan struct{}
	log        *logrus.Logger
}

type Config struct {
	MaxRecvSize int
}

func NewServer(log *logrus.Logger, config Config) *Server {
	return &Server{
		config:     config,
		shutdownCh: make(chan struct{}),
		log:        log,
	}
}

func (s *Server) Serve(ln net.Listener, engine *backend.Engine) error {
	for {
		conn, err := ln.Accept()

		// stop accepting connection on shutdown
		select {
		case <-s.shutdownCh:
			return ErrServerClosed
		default:
		}

		if err != nil {
			s.log.WithError(err).Error("error acceping new connection")
			// TODO: prevent mass amounts of errors with backoff and or closing the server completely
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
	defer conn.Close()
	s.log.Infof("connect: %+v", conn.RemoteAddr())

	dbConn := NewConnection(s.log, engine.NewPager(), conn)

	// 1 byte for control
	// 4 bytes for payload length
	controlHeader := [5]byte{}

	// buffer for handling payloads
	payloadBuffer := make([]byte, 0, s.config.MaxRecvSize)

	// TODO: handle errors gracefully rather than closing connection
	for {
		n, err := dbConn.Read(controlHeader[:])
		if err != nil {
			s.log.Error("error reading control header")
			return
		}

		// not enough bytes read in control header
		if n != len(controlHeader) {
			s.log.Error("invalid control header length")
			return
		}

		// read payload
		payloadLen := binary.BigEndian.Uint32(controlHeader[1:])
		if int(payloadLen) > s.config.MaxRecvSize {
			s.log.Error("invalid payload size")
			return
		}

		if payloadLen > 0 {
			payloadRead, err := conn.Read(payloadBuffer[:payloadLen])
			if err != nil {
				s.log.WithError(err).Error("error reading payload")
				return
			}
			if payloadRead != int(payloadLen) {
				s.log.Errorf("error reading full payload: expected %d got %d", payloadLen, payloadRead)
				return
			}
		}

		// handle the command
		if err := dbConn.Handle(context.Background(), Command{
			Control: Control(controlHeader[0]),
			Payload: payloadBuffer[:payloadLen],
		}); err != nil {
			s.log.WithError(err).Error("error handling command")
			return
		}
	}
}
