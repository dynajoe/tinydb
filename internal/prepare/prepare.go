package prepare

import (
	"fmt"

	"github.com/joeandaverde/tinydb/internal/metadata"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/joeandaverde/tinydb/internal/virtualmachine"
	"github.com/joeandaverde/tinydb/tsql"
	"github.com/joeandaverde/tinydb/tsql/ast"
)

// Prepare compiles a statement into a set of instructions to run in the database virtual machine.
func Prepare(text string, pager storage.Pager) ([]virtualmachine.Instruction, error) {
	stmt, err := tsql.Parse(text)
	if err != nil {
		return nil, err
	}

	switch s := stmt.(type) {
	case *ast.CreateTableStatement:
		return CreateTableInstructions(s), nil
	case *ast.InsertStatement:
		return InsertInstructions(pager, s), nil
	case *ast.SelectStatement:
		table, err := metadata.GetTableDefinition(pager, s.From[0].Name)
		if err != nil {
			return nil, err
		}
		tableLookup := make(map[string]*metadata.TableDefinition)
		tableLookup[table.Name] = table

		return SelectInstructions(tableLookup, s), nil
	default:
		return nil, fmt.Errorf("unexpected statement type")
	}
}
