package backend

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"sync"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
	"github.com/joeandaverde/tinydb/tsql"
)

type Backend struct {
	sync.Mutex

	pager      pager.Pager
	pidCounter int
	inTx       bool
	failed     bool
	proc       chan struct{}
	log        *logrus.Logger
}

// Row is a row in a result
type Row struct {
	Data []interface{}
}

type exitCode byte

const (
	exitCodeBegin exitCode = iota
	exitCodeCommit
	exitCodeRollback
	exitCodeError
)

type ProgramInstance struct {
	Pid    int
	Tag    string
	Output <-chan virtualmachine.Output

	Exit    <-chan error
	inTx    bool
	program *virtualmachine.Program
	pager   pager.Pager
}

func NewBackend(logger *logrus.Logger, p pager.Pager) *Backend {
	sema := make(chan struct{}, 1)
	sema <- struct{}{}

	return &Backend{
		pager:      p,
		pidCounter: 0,
		proc:       sema,
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
	// reserve the processor
	<-b.proc

	if b.failed {
		return nil, fmt.Errorf("backend in failure state and requires reset")
	}

	b.pidCounter++

	pid := b.pidCounter
	program := virtualmachine.NewProgram(pid, stmt)

	// ready program for execution
	exitCh := make(chan error, 1)
	instance := &ProgramInstance{
		Pid:     pid,
		Output:  program.Output(),
		Exit:    exitCh,
		Tag:     stmt.Tag,
		inTx:    b.inTx,
		pager:   b.pager,
		program: program,
	}

	go func() {
		defer close(exitCh)

		// release processor reservation
		defer func() { b.proc <- struct{}{} }()

		b.log.Debugf("running program")

		c, err := run(ctx, instance)
		if err != nil {
			b.log.WithError(err).Error("program error")
			b.failed = true
			exitCh <- err
			return
		}

		switch c {
		case exitCodeBegin:
			b.log.Debugf("program exit: begin")
			b.begin()
			return
		case exitCodeCommit:
			b.log.Debugf("program exit: commit")
			exitCh <- b.commit()
			return
		case exitCodeRollback:
			b.log.Debugf("program exit: rollback")
			b.rollback()
			return
		}

		b.log.WithError(err).Errorf("program exit: unexpected exit code %d", c)
		b.rollback()
		exitCh <- fmt.Errorf("unexpected program exit code: %d", c)
	}()

	return instance, nil
}

// run runs a program and returns an exit code
func run(ctx context.Context, instance *ProgramInstance) (exitCode, error) {
	flags, err := instance.program.Run(ctx, virtualmachine.Flags{
		AutoCommit: !instance.inTx,
		Rollback:   false,
	}, instance.pager)
	if err != nil {
		return exitCodeError, err
	}

	if flags.Rollback {
		return exitCodeRollback, nil
	}

	if flags.AutoCommit {
		return exitCodeCommit, nil
	}

	return exitCodeBegin, nil
}

// rollback rolls back any changes made during the program execution
func (b *Backend) rollback() {
	b.inTx = false
	b.log.Debug("rollback")
	b.pager.Reset()
}

// commit ensures modifications are persisted
func (b *Backend) commit() error {
	b.inTx = false
	b.log.Debug("commit")
	if err := b.pager.Flush(); err != nil {
		b.log.WithError(err).Error("commit failed")
		b.rollback()
		return err
	}
	return nil
}

// begin makes no changes to the underlying pager and ensures the backend is in a transacted state
func (b *Backend) begin() {
	b.inTx = true
}
