package ast

// BeginStatement starts a transaction
type BeginStatement struct{}

// CommitStatement commits a transaction
type CommitStatement struct{}

// RollbackStatement rolls back a transaction
type RollbackStatement struct{}

func (*BeginStatement) iStatement()    {}
func (*CommitStatement) iStatement()   {}
func (*RollbackStatement) iStatement() {}

func (*BeginStatement) Mutates() bool    { return false }
func (*CommitStatement) Mutates() bool   { return false }
func (*RollbackStatement) Mutates() bool { return false }
