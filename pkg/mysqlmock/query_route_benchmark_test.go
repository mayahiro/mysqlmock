package mysqlmock

import (
	"strings"
	"testing"
)

type benchmarkCOMQueryRoute string

const (
	benchmarkCOMQueryRouteError       benchmarkCOMQueryRoute = "error"
	benchmarkCOMQueryRouteRules       benchmarkCOMQueryRoute = "rules"
	benchmarkCOMQueryRouteCompat      benchmarkCOMQueryRoute = "compat"
	benchmarkCOMQueryRouteSQLiteRead  benchmarkCOMQueryRoute = "sqlite_read"
	benchmarkCOMQueryRouteSQLiteWrite benchmarkCOMQueryRoute = "sqlite_write"
	benchmarkCOMQueryRouteUnsupported benchmarkCOMQueryRoute = "unsupported"
)

var (
	benchmarkCOMQueryRouteSink benchmarkCOMQueryRoute
	benchmarkCOMQueryErrSink   error
)

func BenchmarkCOMQueryRouteDecision(b *testing.B) {
	plainConn := benchmarkCOMQueryRouteConn(b, 0)
	rules10Conn := benchmarkCOMQueryRouteConn(b, 10)
	rules100Conn := benchmarkCOMQueryRouteConn(b, 100)

	benchmarks := []struct {
		name string
		conn *mysqlConn
		sql  string
		want benchmarkCOMQueryRoute
	}{
		{
			name: "no_rules/compat_version",
			conn: plainConn,
			sql:  "SELECT VERSION()",
			want: benchmarkCOMQueryRouteCompat,
		},
		{
			name: "no_rules/compat_set_names",
			conn: plainConn,
			sql:  "SET NAMES utf8mb4 COLLATE utf8mb4_bin",
			want: benchmarkCOMQueryRouteCompat,
		},
		{
			name: "no_rules/compat_information_schema",
			conn: plainConn,
			sql:  "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
			want: benchmarkCOMQueryRouteCompat,
		},
		{
			name: "no_rules/sqlite_read",
			conn: plainConn,
			sql:  "SELECT id, name FROM users WHERE email = ? ORDER BY id DESC LIMIT 1",
			want: benchmarkCOMQueryRouteSQLiteRead,
		},
		{
			name: "no_rules/sqlite_write",
			conn: plainConn,
			sql:  "INSERT INTO users (name, email) VALUES (?, ?)",
			want: benchmarkCOMQueryRouteSQLiteWrite,
		},
		{
			name: "no_rules/unsupported",
			conn: plainConn,
			sql:  "CREATE USER benchmark_user",
			want: benchmarkCOMQueryRouteUnsupported,
		},
		{
			name: "rules_10/hit_last",
			conn: rules10Conn,
			sql:  "SELECT benchmark_route_rule_9",
			want: benchmarkCOMQueryRouteRules,
		},
		{
			name: "rules_10/miss_sqlite_read",
			conn: rules10Conn,
			sql:  "SELECT id FROM users WHERE id = ?",
			want: benchmarkCOMQueryRouteSQLiteRead,
		},
		{
			name: "rules_100/hit_last",
			conn: rules100Conn,
			sql:  "SELECT benchmark_route_rule_99",
			want: benchmarkCOMQueryRouteRules,
		},
		{
			name: "rules_100/miss_sqlite_read",
			conn: rules100Conn,
			sql:  "SELECT id FROM users WHERE id = ?",
			want: benchmarkCOMQueryRouteSQLiteRead,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			route, err := benchmarkClassifyCOMQueryRoute(bm.conn, bm.sql)
			if err != nil {
				b.Fatalf("classify route: %v", err)
			}
			if route != bm.want {
				b.Fatalf("route = %s, want %s", route, bm.want)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				route, err = benchmarkClassifyCOMQueryRoute(bm.conn, bm.sql)
			}
			benchmarkCOMQueryRouteSink = route
			benchmarkCOMQueryErrSink = err
		})
	}
}

func benchmarkCOMQueryRouteConn(b *testing.B, ruleCount int) *mysqlConn {
	b.Helper()

	rules := make([]RuleConfig, ruleCount)
	for i := range rules {
		value := stringForCacheTestInt(i)
		rules[i] = RuleConfig{
			Name: "benchmark route rule " + value,
			Request: RuleRequestConfig{
				Match: "exact",
				SQL:   "SELECT benchmark_route_rule_" + value,
			},
			Response: RuleResponseConfig{Type: "ok"},
		}
	}

	prepared, err := prepareRules(rules)
	if err != nil {
		b.Fatalf("prepare rules: %v", err)
	}
	return &mysqlConn{
		server: &Server{
			cfg:   DefaultConfig(),
			rules: prepared,
		},
		currentDB: "mysqlmock",
	}
}

func benchmarkClassifyCOMQueryRoute(c *mysqlConn, sqlText string, args ...any) (benchmarkCOMQueryRoute, error) {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return benchmarkCOMQueryRouteError, errPacket(mysqlErrUnknown, "HY000", "Unsupported query: empty SQL")
	}
	normalized := normalizeSQL(trimmed)
	upper := strings.ToUpper(normalized)
	_, isVersionQuery := selectVersionColumnName(trimmed)

	if _, matched, err := c.server.matchRule(sqlText, args); matched || err != nil {
		return benchmarkCOMQueryRouteRules, err
	}

	switch {
	case strings.HasPrefix(upper, "SET NAMES "):
		return benchmarkCOMQueryRouteCompat, nil
	case strings.HasPrefix(upper, "SET AUTOCOMMIT"):
		return benchmarkCOMQueryRouteCompat, nil
	case strings.HasPrefix(upper, "SET TRANSACTION "):
		return benchmarkCOMQueryRouteCompat, nil
	case strings.HasPrefix(upper, "SET "):
		return benchmarkCOMQueryRouteCompat, nil
	case isVersionQuery:
		return benchmarkCOMQueryRouteCompat, nil
	case isAdvisoryLockQuery(upper):
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SELECT DATABASE()":
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SELECT SCHEMA()":
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SELECT USER()":
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SELECT CURRENT_USER()" || upper == "SELECT CURRENT_USER":
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SELECT CONNECTION_ID()":
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SELECT LAST_INSERT_ID()":
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SELECT ROW_COUNT()":
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SELECT @@VERSION" || upper == "SELECT @@SESSION.VERSION" || upper == "SELECT @@GLOBAL.VERSION":
		return benchmarkCOMQueryRouteCompat, nil
	case strings.HasPrefix(upper, "SELECT @@"):
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SHOW VARIABLES":
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "SHOW TABLES":
		return benchmarkCOMQueryRouteCompat, nil
	case isShowFullFieldsQuery(upper):
		return benchmarkCOMQueryRouteCompat, nil
	case isShowCreateTableQuery(upper):
		return benchmarkCOMQueryRouteCompat, nil
	case isShowKeysQuery(upper):
		return benchmarkCOMQueryRouteCompat, nil
	case isInformationSchemaQuery(upper):
		return benchmarkCOMQueryRouteCompat, nil
	case upper == "BEGIN" || upper == "START TRANSACTION":
		return benchmarkCOMQueryRouteSQLiteWrite, nil
	case upper == "COMMIT":
		return benchmarkCOMQueryRouteSQLiteWrite, nil
	case upper == "ROLLBACK":
		return benchmarkCOMQueryRouteSQLiteWrite, nil
	case upper == "ROLLBACK AND CHAIN":
		return benchmarkCOMQueryRouteSQLiteWrite, nil
	case strings.HasPrefix(upper, "ROLLBACK TO SAVEPOINT "):
		return benchmarkCOMQueryRouteSQLiteWrite, nil
	case strings.HasPrefix(upper, "SAVEPOINT ") ||
		strings.HasPrefix(upper, "RELEASE SAVEPOINT "):
		return benchmarkCOMQueryRouteSQLiteWrite, nil
	}

	if isReadQuery(upper) {
		return benchmarkCOMQueryRouteSQLiteRead, nil
	}
	if isWriteQuery(upper) {
		return benchmarkCOMQueryRouteSQLiteWrite, nil
	}

	return benchmarkCOMQueryRouteUnsupported, nil
}
