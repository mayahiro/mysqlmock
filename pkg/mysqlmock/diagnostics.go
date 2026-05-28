package mysqlmock

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// QuerySnapshot is the stable JSON representation used for golden files.
type QuerySnapshot struct {
	Queries []QuerySnapshotEntry `json:"queries"`
}

// QuerySnapshotEntry is a query event with volatile fields removed.
type QuerySnapshotEntry struct {
	Command       string `json:"command,omitempty"`
	Route         string `json:"route,omitempty"`
	Database      string `json:"database,omitempty"`
	SQL           string `json:"sql"`
	NormalizedSQL string `json:"normalized_sql,omitempty"`
}

// UnsupportedSnapshot is the stable JSON representation for unsupported SQL.
type UnsupportedSnapshot struct {
	Queries []UnsupportedSnapshotEntry `json:"queries"`
}

// UnsupportedSnapshotEntry is an unsupported query with volatile fields removed.
type UnsupportedSnapshotEntry struct {
	Command       string `json:"command,omitempty"`
	RouteStage    string `json:"route_stage,omitempty"`
	Database      string `json:"database,omitempty"`
	SQL           string `json:"sql"`
	NormalizedSQL string `json:"normalized_sql,omitempty"`
	Suggestion    string `json:"suggestion,omitempty"`
}

// QuerySnapshotJSON returns deterministic JSON for query golden files.
//
// Connection IDs are intentionally omitted because they can vary with client
// pooling behavior. Normalized SQL is omitted when it is identical to SQL.
func QuerySnapshotJSON(queries []QueryEvent) ([]byte, error) {
	snapshot := QuerySnapshot{
		Queries: make([]QuerySnapshotEntry, 0, len(queries)),
	}
	for _, query := range queries {
		normalizedSQL := query.NormalizedSQL
		if normalizedSQL == query.SQL {
			normalizedSQL = ""
		}
		snapshot.Queries = append(snapshot.Queries, QuerySnapshotEntry{
			Command:       query.Command,
			Route:         query.Route,
			Database:      query.Database,
			SQL:           query.SQL,
			NormalizedSQL: normalizedSQL,
		})
	}

	out, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

// QuerySnapshotJSON returns deterministic JSON for query golden files.
func (s *Server) QuerySnapshotJSON() ([]byte, error) {
	return QuerySnapshotJSON(s.Queries())
}

// WriteQuerySnapshot writes deterministic JSON for query golden files.
func WriteQuerySnapshot(path string, queries []QueryEvent) error {
	out, err := QuerySnapshotJSON(queries)
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o644)
}

// UnsupportedSnapshotJSON returns deterministic JSON for unsupported SQL golden files.
func UnsupportedSnapshotJSON(queries []UnsupportedQuery) ([]byte, error) {
	snapshot := UnsupportedSnapshot{
		Queries: make([]UnsupportedSnapshotEntry, 0, len(queries)),
	}
	for _, query := range queries {
		normalizedSQL := query.NormalizedSQL
		if normalizedSQL == query.SQL {
			normalizedSQL = ""
		}
		snapshot.Queries = append(snapshot.Queries, UnsupportedSnapshotEntry{
			Command:       query.Command,
			RouteStage:    query.RouteStage,
			Database:      query.CurrentDB,
			SQL:           query.SQL,
			NormalizedSQL: normalizedSQL,
			Suggestion:    query.Suggestion,
		})
	}

	out, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

// UnsupportedSnapshotJSON returns deterministic JSON for unsupported SQL golden files.
func (s *Server) UnsupportedSnapshotJSON() ([]byte, error) {
	return UnsupportedSnapshotJSON(s.Unsupported())
}

// WriteUnsupportedSnapshot writes deterministic JSON for unsupported SQL golden files.
func WriteUnsupportedSnapshot(path string, queries []UnsupportedQuery) error {
	out, err := UnsupportedSnapshotJSON(queries)
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o644)
}

// AssertNoUnsupported fails t when the server observed unsupported SQL.
func AssertNoUnsupported(t testing.TB, server *Server) {
	t.Helper()

	unsupported := server.Unsupported()
	if len(unsupported) == 0 {
		return
	}

	var out strings.Builder
	for i, query := range unsupported {
		if i > 0 {
			out.WriteString("\n\n")
		}
		out.WriteString(query.Command)
		out.WriteString(" ")
		out.WriteString(query.RouteStage)
		out.WriteString(": ")
		out.WriteString(query.SQL)
		if query.Suggestion != "" {
			out.WriteString("\n")
			out.WriteString(query.Suggestion)
		}
	}
	t.Fatalf("mysqlmock observed %d unsupported queries:\n%s", len(unsupported), out.String())
}

// UnsupportedTemplate returns a YAML rule template for unsupported queries.
func UnsupportedTemplate() string {
	return "rules:\n" +
		"  - name: generated unsupported query\n" +
		"    request:\n" +
		"      match: exact\n" +
		"      sql: \"SELECT @@example\"\n" +
		"    response:\n" +
		"      type: result_set\n" +
		"      columns:\n" +
		"        - name: \"@@example\"\n" +
		"          type: VARCHAR\n" +
		"      rows:\n" +
		"        - [\"TODO\"]"
}
