package mysqlmock_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"
)

func TestQuerySnapshotJSONOmitsVolatileFields(t *testing.T) {
	got, err := mysqlmock.QuerySnapshotJSON([]mysqlmock.QueryEvent{
		{
			Event:         "query",
			ConnectionID:  42,
			Command:       "COM_QUERY",
			Route:         "sqlite",
			Database:      "mysqlmock",
			SQL:           " select  1 ",
			NormalizedSQL: "SELECT 1",
			Suggestion:    "rules:\n  - name: generated unsupported query",
		},
		{
			Event:         "query",
			ConnectionID:  43,
			Command:       "COM_QUERY",
			Route:         "compat",
			Database:      "mysqlmock",
			SQL:           "SELECT VERSION()",
			NormalizedSQL: "SELECT VERSION()",
		},
	})
	if err != nil {
		t.Fatalf("QuerySnapshotJSON: %v", err)
	}

	want := `{
  "queries": [
    {
      "command": "COM_QUERY",
      "route": "sqlite",
      "database": "mysqlmock",
      "sql": " select  1 ",
      "normalized_sql": "SELECT 1"
    },
    {
      "command": "COM_QUERY",
      "route": "compat",
      "database": "mysqlmock",
      "sql": "SELECT VERSION()"
    }
  ]
}
`
	if string(got) != want {
		t.Fatalf("QuerySnapshotJSON() = %q, want %q", got, want)
	}
	for _, omitted := range []string{"connection_id", `"event"`, "Suggestion", "generated unsupported query"} {
		if strings.Contains(string(got), omitted) {
			t.Fatalf("QuerySnapshotJSON() contains volatile field %q: %s", omitted, got)
		}
	}
}

func TestWriteQuerySnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.golden.json")
	if err := mysqlmock.WriteQuerySnapshot(path, []mysqlmock.QueryEvent{{SQL: "SELECT 1"}}); err != nil {
		t.Fatalf("WriteQuerySnapshot: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if !strings.Contains(string(got), `"sql": "SELECT 1"`) {
		t.Fatalf("snapshot file = %s", got)
	}
}

func TestUnsupportedSnapshotJSONIncludesRuleSuggestion(t *testing.T) {
	got, err := mysqlmock.UnsupportedSnapshotJSON([]mysqlmock.UnsupportedQuery{
		{
			SQL:           " SELECT   @@session.unknown_variable ;",
			NormalizedSQL: "SELECT @@session.unknown_variable",
			ConnectionID:  42,
			Command:       "COM_QUERY",
			CurrentDB:     "mysqlmock",
			RouteStage:    "compat",
			Suggestion:    "rules:\n  - name: generated unsupported query",
		},
	})
	if err != nil {
		t.Fatalf("UnsupportedSnapshotJSON: %v", err)
	}

	want := `{
  "queries": [
    {
      "command": "COM_QUERY",
      "route_stage": "compat",
      "database": "mysqlmock",
      "sql": " SELECT   @@session.unknown_variable ;",
      "normalized_sql": "SELECT @@session.unknown_variable",
      "suggestion": "rules:\n  - name: generated unsupported query"
    }
  ]
}
`
	if string(got) != want {
		t.Fatalf("UnsupportedSnapshotJSON() = %q, want %q", got, want)
	}
	if strings.Contains(string(got), "connection_id") {
		t.Fatalf("UnsupportedSnapshotJSON() contains connection_id: %s", got)
	}
}

func TestWriteUnsupportedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unsupported.golden.json")
	if err := mysqlmock.WriteUnsupportedSnapshot(path, []mysqlmock.UnsupportedQuery{{SQL: "CREATE USER example"}}); err != nil {
		t.Fatalf("WriteUnsupportedSnapshot: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read unsupported snapshot: %v", err)
	}
	if !strings.Contains(string(got), `"sql": "CREATE USER example"`) {
		t.Fatalf("unsupported snapshot file = %s", got)
	}
}
