package command

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/joeandaverde/tinydb/engine"
	"gopkg.in/yaml.v2"
)

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

	cmdFlags := flag.NewFlagSet("listen", flag.PanicOnError)
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
	config := &engine.Config{}
	if err := configDecoder.Decode(config); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error parsing config file: %s", err.Error())
		return 1
	}

	ln, err := net.Listen("tcp", config.Addr)

	if err != nil {
		return 1
	}
	defer ln.Close()

	dbEngine, err := engine.Start(config)
	if err != nil {
		return 1
	}

	for {
		conn, err := ln.Accept()
		dbEngine.Log.Infof("client connected remote: %v, local: %v", conn.RemoteAddr(), conn.LocalAddr())

		select {
		case <-i.ShutDownCh:
			return 0
		default:
		}

		if err == nil {
			go handleConnection(dbEngine, conn, i.ShutDownCh)
		}
	}
}

func handleConnection(dbEngine *engine.Engine, conn net.Conn, shutdownCh <-chan struct{}) {
	defer func() {
		dbEngine.Log.Infof("client disconnected remote: %v, local: %v", conn.RemoteAddr(), conn.LocalAddr())
		defer conn.Close()
	}()

	scanner := bufio.NewScanner(conn)

	scanner.Split(onSemicolon)

	for scanner.Scan() {
		select {
		case <-shutdownCh:
			return
		default:
		}

		text := scanner.Text()

		if len(strings.TrimSpace(text)) == 0 {
			continue
		}

		result, err := dbEngine.Command(text)
		writer := bufio.NewWriter(conn)

		if err != nil {
			dbEngine.Log.Error(err)
			_, _ = writer.WriteString(err.Error())
			continue
		}

		for r := range result.Rows {
			_, _ = writer.WriteString(fmt.Sprintf("%s\n", r.Data))
		}

		writer.Flush()
	}

	if err := scanner.Err(); err != nil {
		dbEngine.Log.Errorf("Connection Error: %s", err.Error())
	}
}

func onSemicolon(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for i := 0; i < len(data); i++ {
		if data[i] == ';' {
			return i + 1, data[:i], nil
		}
	}

	if atEOF {
		return len(data), data, bufio.ErrFinalToken
	}

	return 0, nil, nil
}
