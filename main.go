package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
)

func main() {
	scanner := bufio.NewScanner(bufio.NewReader(os.Stdin))
	data, _ := ioutil.ReadFile("./data.sql")

	if data != nil {
		run(string(data))
	} else {
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
}

func run(text string) {
	result := Parse(text)

	if result != nil {
		ExecuteStatement(result)
	}
}
