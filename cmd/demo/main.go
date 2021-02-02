package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/joeandaverde/tinydb/engine"
)

func main() {
	data, _ := ioutil.ReadFile("./samples/select.sql")

	var reader io.Reader

	if data != nil {
		reader = strings.NewReader(string(data))
	} else {
		reader = bufio.NewReader(os.Stdin)
	}

	scanner := bufio.NewScanner(reader)

	onSemicolon := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		for i := 0; i < len(data); i++ {
			if data[i] == ';' {
				return i + 1, data[:i], nil
			}
		}

		return 0, data, bufio.ErrFinalToken
	}

	dbEngine := engine.Start(&engine.Config{
		DataDir: ".",
	})

	scanner.Split(onSemicolon)
	for scanner.Scan() {
		text := scanner.Text()

		if len(strings.TrimSpace(text)) == 0 {
			continue
		}

		_, err := dbEngine.Command(text)

		if err != nil {
			dbEngine.Log.Errorf("Query Error: %s", err.Error())
		}
	}

	if err := scanner.Err(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "reading input:", err)
	}
}
