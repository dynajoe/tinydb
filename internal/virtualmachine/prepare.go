package virtualmachine

import (
	"fmt"

	"github.com/joeandaverde/tinydb/internal/metadata"
	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/tsql/ast"
)

type PreparedStatement struct {
	Statement    ast.Statement
	Tag          string
	Columns      []string
	Instructions []*Instruction
}

// Prepare compiles a statement into a set of instructions to run in the database virtual machine.
func Prepare(stmt ast.Statement, pager pager.Pager) (*PreparedStatement, error) {
	preparedStatement := &PreparedStatement{
		Statement: stmt,
	}

	switch s := stmt.(type) {
	case *ast.CreateTableStatement:
		preparedStatement.Tag = "CREATE"
		preparedStatement.Instructions = CreateTableInstructions(s)
	case *ast.InsertStatement:
		preparedStatement.Tag = "INSERT"
		preparedStatement.Columns = s.Returning
		preparedStatement.Instructions = InsertInstructions(pager, s)
	case *ast.SelectStatement:
		preparedStatement.Tag = "SELECT"
		table, err := metadata.GetTableDefinition(pager, s.From[0].Name)
		if err != nil {
			return nil, err
		}
		tableLookup := make(map[string]*metadata.TableDefinition)
		tableLookup[table.Name] = table

		preparedStatement.Columns = s.Columns
		preparedStatement.Instructions = SelectInstructions(tableLookup, s)
	case *ast.BeginStatement:
		preparedStatement.Tag = "BEGIN"
		preparedStatement.Instructions = BeginInstructions(s)
	case *ast.CommitStatement:
		preparedStatement.Tag = "COMMIT"
		preparedStatement.Instructions = CommitInstructions(s)
	case *ast.RollbackStatement:
		preparedStatement.Tag = "ROLLBACK"
		preparedStatement.Instructions = RollbackInstructions(s)
	default:
		return nil, fmt.Errorf("unexpected statement type")
	}

	return preparedStatement, nil
}
