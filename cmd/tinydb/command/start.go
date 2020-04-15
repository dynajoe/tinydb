package command

import (
	"os"
	"strings"

	"github.com/joeandaverde/tinydb/engine"
)

type StartCommand struct {
	ShutDownCh <-chan struct{}
}

func (i *StartCommand) Help() string {
	helpText := `
Usage: tinydb start
`

	return strings.TrimSpace(helpText)
}

func (i *StartCommand) Synopsis() string {
	return "Starts the database for testing"
}

func (i *StartCommand) Run(args []string) int {
	os.Remove("/Users/joe/Desktop/sqlite/tiny.db")
	db := engine.Start("/Users/joe/Desktop/sqlite/")

	_, err := db.Execute(strings.TrimSpace(`CREATE TABLE person(name text)`))
	if err != nil {
		return 1
	}
	_, err = db.Execute(strings.TrimSpace(`CREATE TABLE company(name text)`))
	if err != nil {
		return 1
	}
	<-i.ShutDownCh
	return 0
}
