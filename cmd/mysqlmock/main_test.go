package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"

	_ "github.com/go-sql-driver/mysql"
)

func TestUnsupportedQueriesError(t *testing.T) {
	if err := unsupportedQueriesError(nil); err != nil {
		t.Fatalf("unsupportedQueriesError(nil) = %v", err)
	}

	err := unsupportedQueriesError([]mysqlmock.UnsupportedQuery{
		{
			SQL:           "CREATE USER unsupported_user",
			NormalizedSQL: "CREATE USER unsupported_user",
			ConnectionID:  7,
			Command:       "COM_QUERY",
			CurrentDB:     "mysqlmock",
			RouteStage:    "unsupported",
			Suggestion: "Suggested rule:\n" +
				"  - name: generated unsupported query",
		},
	})
	if err == nil {
		t.Fatal("expected unsupported queries error")
	}
	for _, want := range []string{
		"unsupported queries observed: 1",
		"CREATE USER unsupported_user",
		"normalized: CREATE USER unsupported_user",
		"connection_id: 7",
		"command: COM_QUERY",
		"database: mysqlmock",
		"route_stage: unsupported",
		"Suggested rule:",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
}

func TestRunDumpConfigSchema(t *testing.T) {
	output := captureStdout(t, func() {
		if err := run([]string{"dump-config-schema"}); err != nil {
			t.Fatalf("dump config schema: %v", err)
		}
	})

	var schema map[string]any
	if err := json.Unmarshal([]byte(output), &schema); err != nil {
		t.Fatalf("dumped config schema is not valid JSON: %v", err)
	}
	if schema["title"] != "mysqlmock config" {
		t.Fatalf("schema title = %#v", schema["title"])
	}
}

func TestWriteServeStats(t *testing.T) {
	server, err := mysqlmock.New(mysqlmock.WithConfig(mysqlmock.DefaultConfig()))
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := writeServeStats(&out, server); err != nil {
		t.Fatalf("write serve stats: %v", err)
	}
	var stats mysqlmock.Stats
	if err := json.Unmarshal(out.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats JSON: %v", err)
	}
	if strings.Contains(out.String(), "SELECT") {
		t.Fatalf("stats JSON contains SQL text: %s", out.String())
	}
}

func TestWaitForServeStopFailsOnUnsupported(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server, err := mysqlmock.New(mysqlmock.WithConfig(mysqlmock.DefaultConfig()))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		done <- waitForServeStop(ctx, server, true, time.Millisecond)
	}()

	db, err := sql.Open("mysql", fmt.Sprintf("user:password@tcp(%s)/mysqlmock?interpolateParams=true", server.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "CREATE USER wait_unsupported"); err == nil {
		t.Fatal("expected unsupported query")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected fail-on-unsupported error")
		}
		if !strings.Contains(err.Error(), "unsupported queries observed: 1") ||
			!strings.Contains(err.Error(), "CREATE USER wait_unsupported") {
			t.Fatalf("unexpected fail-on-unsupported error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for fail-on-unsupported")
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	return string(data)
}
