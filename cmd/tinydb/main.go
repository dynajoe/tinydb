package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/mitchellh/cli"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		args = append(args, "listen")
	}

	commands := map[string]cli.CommandFactory{
		"listen": func() (cli.Command, error) {
			return &ListenCommand{
				ShutDownCh: makeShutdownCh(),
			}, nil
		},
	}

	tinyCLI := &cli.CLI{
		Args:     args,
		Commands: commands,
		HelpFunc: cli.BasicHelpFunc("tinydb"),
	}

	exitCode, err := tinyCLI.Run()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(1)
	}

	os.Exit(exitCode)
}

func makeShutdownCh() <-chan struct{} {
	shutdownCh := make(chan struct{})
	signalCh := make(chan os.Signal)

	signal.Notify(signalCh, os.Interrupt)

	go func() {
		defer close(shutdownCh)
		<-signalCh
	}()

	return shutdownCh
}
