package command

import (
	"io/ioutil"
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
	tempDir, _ := ioutil.TempDir("", "tinydb")
	db := engine.Start(tempDir)

	_, err := db.Execute(strings.TrimSpace(`
	CREATE TABLE person (
		name text,
		company_id int
	)
	`))
	if err != nil {
		return 1
	}
	<-i.ShutDownCh
	return 0
}
