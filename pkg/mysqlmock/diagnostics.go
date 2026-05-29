package mysqlmock

import (
	"os"
	"strings"
	"testing"
	"unicode/utf8"
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
	out := make([]byte, 0, estimateQuerySnapshotJSONSize(queries))
	out = appendSnapshotJSONStart(out, len(queries))
	for _, query := range queries {
		normalizedSQL := query.NormalizedSQL
		if normalizedSQL == query.SQL {
			normalizedSQL = ""
		}

		out = appendSnapshotObjectStart(out)
		fieldCount := 0
		out = appendSnapshotStringField(out, &fieldCount, "command", query.Command, true)
		out = appendSnapshotStringField(out, &fieldCount, "route", query.Route, true)
		out = appendSnapshotStringField(out, &fieldCount, "database", query.Database, true)
		out = appendSnapshotStringField(out, &fieldCount, "sql", query.SQL, false)
		out = appendSnapshotStringField(out, &fieldCount, "normalized_sql", normalizedSQL, true)
		out = appendSnapshotObjectEnd(out)
	}
	out = appendSnapshotJSONEnd(out, len(queries))
	return out, nil
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
	out := make([]byte, 0, estimateUnsupportedSnapshotJSONSize(queries))
	out = appendSnapshotJSONStart(out, len(queries))
	for _, query := range queries {
		normalizedSQL := query.NormalizedSQL
		if normalizedSQL == query.SQL {
			normalizedSQL = ""
		}

		out = appendSnapshotObjectStart(out)
		fieldCount := 0
		out = appendSnapshotStringField(out, &fieldCount, "command", query.Command, true)
		out = appendSnapshotStringField(out, &fieldCount, "route_stage", query.RouteStage, true)
		out = appendSnapshotStringField(out, &fieldCount, "database", query.CurrentDB, true)
		out = appendSnapshotStringField(out, &fieldCount, "sql", query.SQL, false)
		out = appendSnapshotStringField(out, &fieldCount, "normalized_sql", normalizedSQL, true)
		out = appendSnapshotStringField(out, &fieldCount, "suggestion", query.Suggestion, true)
		out = appendSnapshotObjectEnd(out)
	}
	out = appendSnapshotJSONEnd(out, len(queries))
	return out, nil
}

func appendSnapshotJSONStart(out []byte, count int) []byte {
	if count == 0 {
		return append(out, "{\n  \"queries\": []\n}\n"...)
	}
	return append(out, "{\n  \"queries\": [\n"...)
}

func appendSnapshotJSONEnd(out []byte, count int) []byte {
	if count == 0 {
		return out
	}
	return append(out, "\n  ]\n}\n"...)
}

func appendSnapshotObjectStart(out []byte) []byte {
	if len(out) > len("{\n  \"queries\": [\n") {
		out = append(out, ",\n"...)
	}
	return append(out, "    {\n"...)
}

func appendSnapshotObjectEnd(out []byte) []byte {
	return append(out, "\n    }"...)
}

func appendSnapshotStringField(out []byte, fieldCount *int, name, value string, omitEmpty bool) []byte {
	if omitEmpty && value == "" {
		return out
	}
	if *fieldCount > 0 {
		out = append(out, ",\n"...)
	}
	out = append(out, "      \""...)
	out = append(out, name...)
	out = append(out, "\": "...)
	out = appendSnapshotJSONString(out, value)
	*fieldCount++
	return out
}

func estimateQuerySnapshotJSONSize(queries []QueryEvent) int {
	if len(queries) == 0 {
		return len("{\n  \"queries\": []\n}\n")
	}

	size := len("{\n  \"queries\": [\n") + len("\n  ]\n}\n")
	for i, query := range queries {
		if i > 0 {
			size += len(",\n")
		}
		normalizedSQL := query.NormalizedSQL
		if normalizedSQL == query.SQL {
			normalizedSQL = ""
		}
		size += len("    {\n") + len("\n    }")
		fieldCount := 0
		size = estimateSnapshotStringField(size, &fieldCount, "command", query.Command, true)
		size = estimateSnapshotStringField(size, &fieldCount, "route", query.Route, true)
		size = estimateSnapshotStringField(size, &fieldCount, "database", query.Database, true)
		size = estimateSnapshotStringField(size, &fieldCount, "sql", query.SQL, false)
		size = estimateSnapshotStringField(size, &fieldCount, "normalized_sql", normalizedSQL, true)
	}
	return size
}

func estimateUnsupportedSnapshotJSONSize(queries []UnsupportedQuery) int {
	if len(queries) == 0 {
		return len("{\n  \"queries\": []\n}\n")
	}

	size := len("{\n  \"queries\": [\n") + len("\n  ]\n}\n")
	for i, query := range queries {
		if i > 0 {
			size += len(",\n")
		}
		normalizedSQL := query.NormalizedSQL
		if normalizedSQL == query.SQL {
			normalizedSQL = ""
		}
		size += len("    {\n") + len("\n    }")
		fieldCount := 0
		size = estimateSnapshotStringField(size, &fieldCount, "command", query.Command, true)
		size = estimateSnapshotStringField(size, &fieldCount, "route_stage", query.RouteStage, true)
		size = estimateSnapshotStringField(size, &fieldCount, "database", query.CurrentDB, true)
		size = estimateSnapshotStringField(size, &fieldCount, "sql", query.SQL, false)
		size = estimateSnapshotStringField(size, &fieldCount, "normalized_sql", normalizedSQL, true)
		size = estimateSnapshotStringField(size, &fieldCount, "suggestion", query.Suggestion, true)
	}
	return size
}

func estimateSnapshotStringField(size int, fieldCount *int, name, value string, omitEmpty bool) int {
	if omitEmpty && value == "" {
		return size
	}
	if *fieldCount > 0 {
		size += len(",\n")
	}
	size += len("      \"") + len(name) + len("\": ") + snapshotJSONStringLen(value)
	*fieldCount++
	return size
}

func appendSnapshotJSONString(out []byte, value string) []byte {
	out = append(out, '"')
	start := 0
	for i := 0; i < len(value); {
		b := value[i]
		if b < utf8.RuneSelf {
			if snapshotJSONSafeASCII(b) {
				i++
				continue
			}
			out = append(out, value[start:i]...)
			out = appendSnapshotJSONEscapedASCII(out, b)
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(value[i:])
		if r == utf8.RuneError && size == 1 {
			out = append(out, value[start:i]...)
			out = append(out, `\ufffd`...)
			i += size
			start = i
			continue
		}
		if r == '\u2028' || r == '\u2029' {
			out = append(out, value[start:i]...)
			out = append(out, '\\', 'u', '2', '0', '2', snapshotJSONHex[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	out = append(out, value[start:]...)
	out = append(out, '"')
	return out
}

func snapshotJSONStringLen(value string) int {
	size := 2
	for i := 0; i < len(value); {
		b := value[i]
		if b < utf8.RuneSelf {
			if snapshotJSONSafeASCII(b) {
				size++
			} else {
				size += snapshotJSONEscapedASCIILen(b)
			}
			i++
			continue
		}
		r, runeSize := utf8.DecodeRuneInString(value[i:])
		if r == utf8.RuneError && runeSize == 1 {
			size += len(`\ufffd`)
			i += runeSize
			continue
		}
		if r == '\u2028' || r == '\u2029' {
			size += len(`\u2028`)
			i += runeSize
			continue
		}
		size += runeSize
		i += runeSize
	}
	return size
}

func snapshotJSONSafeASCII(b byte) bool {
	return b >= 0x20 && b != '\\' && b != '"' && b != '<' && b != '>' && b != '&'
}

func appendSnapshotJSONEscapedASCII(out []byte, b byte) []byte {
	switch b {
	case '\\', '"':
		return append(out, '\\', b)
	case '\b':
		return append(out, '\\', 'b')
	case '\f':
		return append(out, '\\', 'f')
	case '\n':
		return append(out, '\\', 'n')
	case '\r':
		return append(out, '\\', 'r')
	case '\t':
		return append(out, '\\', 't')
	default:
		return append(out, '\\', 'u', '0', '0', snapshotJSONHex[b>>4], snapshotJSONHex[b&0xF])
	}
}

func snapshotJSONEscapedASCIILen(b byte) int {
	switch b {
	case '\\', '"', '\b', '\f', '\n', '\r', '\t':
		return 2
	default:
		return len(`\u00ff`)
	}
}

const snapshotJSONHex = "0123456789abcdef"

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
