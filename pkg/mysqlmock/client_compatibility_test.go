package mysqlmock_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"
)

func TestMultiLanguageClientCompatibilityCommands(t *testing.T) {
	raw := os.Getenv("MYSQLMOCK_CLIENT_COMPAT_COMMANDS")
	if raw == "" {
		t.Skip("set MYSQLMOCK_CLIENT_COMPAT_COMMANDS to run external client compatibility commands")
	}

	var commands []clientCompatibilityCommand
	if err := json.Unmarshal([]byte(raw), &commands); err != nil {
		t.Fatalf("parse MYSQLMOCK_CLIENT_COMPAT_COMMANDS: %v", err)
	}
	if len(commands) == 0 {
		t.Fatal("MYSQLMOCK_CLIENT_COMPAT_COMMANDS must contain at least one command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(mysqlmock.DefaultConfig()))
	host, port, err := net.SplitHostPort(server.Addr())
	if err != nil {
		t.Fatalf("split mysqlmock address: %v", err)
	}

	for i, spec := range commands {
		name := spec.Name
		if name == "" {
			name = fmt.Sprintf("command_%d", i+1)
		}
		t.Run(name, func(t *testing.T) {
			if len(spec.Command) == 0 {
				t.Fatal("command must not be empty")
			}

			cmd := exec.CommandContext(ctx, spec.Command[0], spec.Command[1:]...)
			cmd.Env = append(os.Environ(),
				"MYSQLMOCK_ADDR="+server.Addr(),
				"MYSQLMOCK_HOST="+host,
				"MYSQLMOCK_PORT="+port,
				"MYSQLMOCK_USER=user",
				"MYSQLMOCK_PASSWORD=password",
				"MYSQLMOCK_DATABASE=mysqlmock",
				"MYSQLMOCK_DSN="+server.DSN(),
			)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("external client command failed: %v\n%s", err, strings.TrimSpace(string(output)))
			}
		})
	}
}

type clientCompatibilityCommand struct {
	Name    string   `json:"name"`
	Command []string `json:"command"`
}
