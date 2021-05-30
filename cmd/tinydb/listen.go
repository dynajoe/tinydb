package main

import (
	"flag"
	"fmt"
	"github.com/joeandaverde/tinydb/internal/server"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/joeandaverde/tinydb/internal/backend"
)

type ListenConfig struct {
	Addr     string       `yaml:"addr"`
	DataDir  string       `yaml:"data_directory"`
	PageSize int          `yaml:"page_size"`
	LogLevel logrus.Level `yaml:"log_level"`
}

type ListenCommand struct {
	ShutDownCh <-chan struct{}
}

func (i *ListenCommand) Help() string {
	helpText := `
Usage: tinydb listen [options]

Options:

	-config=""	Database configuration file
`

	return strings.TrimSpace(helpText)
}

func (i *ListenCommand) Synopsis() string {
	return "Accepts client connections to interact with database"
}

func (i *ListenCommand) Run(args []string) int {
	var configPath string

	cmdFlags := flag.NewFlagSet("listen", flag.ExitOnError)
	cmdFlags.StringVar(&configPath, "config", ".", "config file")

	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	configFile, err := os.Open(configPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error opening config file: %s", err.Error())
		return 1
	}

	configDecoder := yaml.NewDecoder(configFile)
	config := &ListenConfig{}
	if err := configDecoder.Decode(config); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error parsing config file: %s", err.Error())
		return 1
	}

	logger := logrus.New()
	logger.SetLevel(config.LogLevel)

	ln, err := net.Listen("tcp", config.Addr)
	if err != nil {
		return 1
	}
	defer ln.Close()

	dbEngine, err := backend.Start(logger, backend.Config{
		DataDir:  config.DataDir,
		PageSize: 4096,
	})
	if err != nil {
		return 1
	}

	dbServer := server.NewServer(logger, server.Config{
		MaxRecvSize: 512,
	})

	if err := dbServer.Serve(ln, dbEngine); err != nil {
		return 1
	}

	return 0
}
