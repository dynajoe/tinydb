package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

func main() {
	Parse("(1+6)*7")
	return
	data, _ := ioutil.ReadFile("./sample_query.sql")

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

	scanner.Split(onSemicolon)

	for scanner.Scan() {
		run(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}
}

func run(text string) {
	result := Parse(strings.TrimSpace(text))

	if result != nil {
		ExecuteStatement(result)
	}
}
