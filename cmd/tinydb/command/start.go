package command

import (
	"fmt"
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
	db := engine.Start(&engine.Config{
		DataDir: "/Users/joe/Desktop/sqlite/",
	})

	_, err := db.Command(strings.TrimSpace(`CREATE TABLE person(name text)`))
	if err != nil {
		return 1
	}
	_, err = db.Command(strings.TrimSpace(`CREATE TABLE company(name text)`))
	if err != nil {
		return 1
	}
	_, err = db.Command("INSERT INTO company (name) VALUES ('hashicorp')")
	if err != nil {
		return 1
	}
	_, err = db.Command("INSERT INTO company (name) VALUES ('smrxt')")
	if err != nil {
		return 1
	}
	results, err := db.Command("SELECT * FROM company WHERE name = 'hashicorp'")
	if err != nil {
		return 1
	}
	for r := range results.Rows {
		fmt.Println(r.Data)
	}
	<-i.ShutDownCh
	return 0
}
