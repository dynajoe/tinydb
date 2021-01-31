package command

import (
	"fmt"
	"os"
	"strings"

	"github.com/joeandaverde/tinydb/engine"
	"github.com/joeandaverde/tinydb/internal/interpret"
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
	db := engine.Start(&engine.Config{
		DataDir: "/Users/joe/Desktop/sqlite/",
	})

	_, err := interpret.Execute(db, strings.TrimSpace(`CREATE TABLE person(name text)`))
	if err != nil {
		return 1
	}
	_, err = interpret.Execute(db, strings.TrimSpace(`CREATE TABLE company(name text)`))
	if err != nil {
		return 1
	}
	_, err = interpret.Execute(db, "INSERT INTO company (name) VALUES ('hashicorp')")
	if err != nil {
		return 1
	}
	_, err = interpret.Execute(db, "INSERT INTO company (name) VALUES ('smrxt')")
	if err != nil {
		return 1
	}
	results, err := interpret.Execute(db, "SELECT * FROM company WHERE name = 'hashicorp'")
	if err != nil {
		return 1
	}
	for r := range results.Rows {
		fmt.Println(r.Data)
	}
	<-i.ShutDownCh
	return 0
}
