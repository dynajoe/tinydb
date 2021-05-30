package backend

import (
	"context"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
	"github.com/joeandaverde/tinydb/tsql"
)

type Backend struct {
	pager      pager.Pager
	pidCounter int32
	inTx       bool

	log *logrus.Logger
}

// Row is a row in a result
type Row struct {
	Data []interface{}
}

type ProgramInstance struct {
	Pid    int
	Output <-chan virtualmachine.Output
	Exit   <-chan struct{}
}

func NewBackend(logger *logrus.Logger, p pager.Pager) *Backend {
	return &Backend{
		pager:      p,
		pidCounter: 0,
		log:        logger,
		inTx:       false,
	}
}

// Prepare parses and builds a virtual machine program
func (b *Backend) Prepare(command string) (*virtualmachine.PreparedStatement, error) {
	stmt, err := tsql.Parse(command)
	if err != nil {
		return nil, err
	}

	// Prepare the program
	preparedStmt, err := virtualmachine.Prepare(stmt, b.pager)
	if err != nil {
		return nil, err
	}

	return preparedStmt, nil
}

// Exec executes a statement
func (b *Backend) Exec(ctx context.Context, stmt *virtualmachine.PreparedStatement) (*ProgramInstance, error) {
	pid := int(atomic.AddInt32(&b.pidCounter, 1))

	program := virtualmachine.NewProgram(pid, stmt.Instructions)

	completeCh := make(chan struct{})
	instance := &ProgramInstance{
		Pid:    pid,
		Output: program.Output(),
		Exit:   completeCh,
	}

	go func() {
		defer close(completeCh)
		b.run(ctx, program)
	}()

	return instance, nil
}

func (b *Backend) run(ctx context.Context, program *virtualmachine.Program) {
	log := b.log.WithField("pid", program.Pid()).Logger

	log.Debugf("program starting")

	flags, err := program.Run(ctx, virtualmachine.Flags{
		AutoCommit: !b.inTx,
		Rollback:   false,
	}, b.pager)
	if err != nil {
		log.WithError(err).Error("program error")
		b.rollback()
		return
	}

	log.Debugf("program complete")

	if flags.Rollback {
		b.rollback()
		return
	}

	if flags.AutoCommit {
		b.commit()
		return
	}

	b.inTx = true

	return
}

// rollback rolls back any changes made during the program execution
func (b *Backend) rollback() {
	b.log.Debug("rollback")
	b.pager.Reset()
}

// commit ensures modifications are persisted
func (b *Backend) commit() error {
	b.log.Debug("commit")
	if err := b.pager.Flush(); err != nil {
		b.log.WithError(err).Error("commit failed")
		b.rollback()
		return err
	}
	return nil
}
