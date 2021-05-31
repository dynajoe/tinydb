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
	log        logrus.FieldLogger
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

func NewBackend(logger logrus.FieldLogger, p pager.Pager) *Backend {
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

	log := b.log.WithField("pid", pid)
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

		log.Debugf("running program")
		c, err := run(ctx, instance)

		switch c {
		case exitCodeError:
			log.Debugf("program exit: error")
			exitCh <- b.fatal(err)
			return
		case exitCodeBegin:
			log.Debugf("program exit: begin")
			exitCh <- b.begin()
			return
		case exitCodeCommit:
			log.Debugf("program exit: commit")
			exitCh <- b.commit()
			return
		case exitCodeRollback:
			log.Debugf("program exit: rollback")
			exitCh <- b.rollback()
			return
		default:
			log.Debugf("program exit: code %d", c)
			exitCh <- b.fatal(fmt.Errorf("unknown program exit code: %d", c))
			return
		}
	}()

	return instance, nil
}

func (b *Backend) fatal(err error) error {
	log := b.log.WithField("pid", b.pidCounter)
	b.inTx = false
	b.failed = true
	log.WithError(err).Error("fatal error")
	b.pager.Reset()
	return err
}

// rollback rolls back any changes made during the program execution
func (b *Backend) rollback() error {
	log := b.log.WithField("pid", b.pidCounter)

	b.inTx = false
	log.Debug("rollback")
	b.pager.Reset()
	return nil
}

// commit ensures modifications are persisted
func (b *Backend) commit() error {
	log := b.log.WithField("pid", b.pidCounter)

	b.inTx = false
	log.Debug("commit")
	if err := b.pager.Flush(); err != nil {
		log.WithError(err).Error("commit failed")
		b.rollback()
		return err
	}
	return nil
}

// begin makes no changes to the underlying pager and ensures the backend is in a transacted state
func (b *Backend) begin() error {
	log := b.log.WithField("pid", b.pidCounter)
	b.inTx = true
	log.Debug("in transaction")
	return nil
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
