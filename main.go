package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(bufio.NewReader(os.Stdin))

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
		runLexer(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}
}

func runLexer(text string) {
	l := lex("TinySql", text)

	for {
		i := l.nextItem()

		fmt.Println(i)

		if i.token == tsqlEOF || i.token == tsqlError {
			break
		}
	}

	fmt.Println("-----")
}
