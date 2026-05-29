package mysqlmock

import (
	"io"
	"testing"
)

const diagnosticsBenchmarkBatchSize = 4096

var (
	benchmarkQueryEvents        []QueryEvent
	benchmarkUnsupportedQueries []UnsupportedQuery
	benchmarkDiagnosticsJSON    []byte
	benchmarkDiagnosticsErr     error
)

func BenchmarkDiagnostics(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		name := "queries_" + stringForCacheTestInt(count)
		b.Run("snapshot/"+name, func(b *testing.B) {
			server := &Server{}
			for _, event := range benchmarkQueryEventsForDiagnostics(count) {
				server.diagnostics.recordQuery(event)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkQueryEvents = server.Queries()
			}
		})

		b.Run("query_snapshot_json/"+name, func(b *testing.B) {
			queries := benchmarkQueryEventsForDiagnostics(count)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkDiagnosticsJSON, benchmarkDiagnosticsErr = QuerySnapshotJSON(queries)
				if benchmarkDiagnosticsErr != nil {
					b.Fatalf("QuerySnapshotJSON: %v", benchmarkDiagnosticsErr)
				}
			}
		})
	}

	for _, count := range []int{1000, 10000} {
		name := "unsupported_" + stringForCacheTestInt(count)
		b.Run("unsupported_snapshot/"+name, func(b *testing.B) {
			server := &Server{}
			for _, query := range benchmarkUnsupportedQueriesForDiagnostics(count) {
				server.diagnostics.recordUnsupported(query)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkUnsupportedQueries = server.Unsupported()
			}
		})

		b.Run("unsupported_snapshot_json/"+name, func(b *testing.B) {
			queries := benchmarkUnsupportedQueriesForDiagnostics(count)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkDiagnosticsJSON, benchmarkDiagnosticsErr = UnsupportedSnapshotJSON(queries)
				if benchmarkDiagnosticsErr != nil {
					b.Fatalf("UnsupportedSnapshotJSON: %v", benchmarkDiagnosticsErr)
				}
			}
		})
	}

	for _, format := range []string{"none", "text", "json"} {
		b.Run("log_query/"+format, func(b *testing.B) {
			server := benchmarkDiagnosticsLogServer(format)
			event := benchmarkQueryEventsForDiagnostics(1)[0]

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				server.logQuery(event)
				if i > 0 && i%diagnosticsBenchmarkBatchSize == 0 {
					b.StopTimer()
					resetBenchmarkDiagnosticsQueries(server)
					b.StartTimer()
				}
			}
		})
	}
}

func benchmarkQueryEventsForDiagnostics(count int) []QueryEvent {
	queries := make([]QueryEvent, count)
	for i := range queries {
		value := stringForCacheTestInt(i)
		queries[i] = QueryEvent{
			Event:         "query",
			ConnectionID:  uint32(i + 1),
			Command:       "COM_QUERY",
			Route:         "sqlite",
			Database:      "mysqlmock",
			SQL:           "SELECT id, name FROM users WHERE id = " + value,
			NormalizedSQL: "SELECT id, name FROM users WHERE id = " + value,
		}
	}
	return queries
}

func benchmarkUnsupportedQueriesForDiagnostics(count int) []UnsupportedQuery {
	queries := make([]UnsupportedQuery, count)
	for i := range queries {
		value := stringForCacheTestInt(i)
		queries[i] = UnsupportedQuery{
			SQL:           "CREATE USER unsupported_" + value,
			NormalizedSQL: "CREATE USER unsupported_" + value,
			ConnectionID:  uint32(i + 1),
			Command:       "COM_QUERY",
			CurrentDB:     "mysqlmock",
			RouteStage:    "unsupported",
			Suggestion:    "rules:\n  - name: generated unsupported query\n    request:\n      match: exact\n      sql: \"CREATE USER unsupported_" + value + "\"",
		}
	}
	return queries
}

func benchmarkDiagnosticsLogServer(format string) *Server {
	server := &Server{logFormat: format}
	switch format {
	case "text", "json":
		server.logWriter = io.Discard
	default:
		server.logFormat = "text"
	}
	return server
}

func resetBenchmarkDiagnosticsQueries(server *Server) {
	server.diagnostics.mu.Lock()
	server.diagnostics.queries = server.diagnostics.queries[:0]
	server.diagnostics.mu.Unlock()
}
