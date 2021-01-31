package virtualmachine

import (
	"github.com/joeandaverde/tinydb/engine"
	"github.com/joeandaverde/tinydb/tsql/ast"
)

func createTable(engine *engine.Engine, createStatement *ast.CreateTableStatement) error {
	instructions := CreateTableInstructions(createStatement)
	program := NewProgram(engine, instructions)
	if err := program.Run(); err != nil {
		return err
	}

	return nil
}
