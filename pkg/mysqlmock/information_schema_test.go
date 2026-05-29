package mysqlmock

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "modernc.org/sqlite"
)

var benchmarkInformationSchemaRows int

func TestRefreshInformationSchemaTableOnlyLoadsTargetTable(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE
);
CREATE INDEX idx_target_users_email ON target_users (email);
CREATE TABLE other_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL
);`); err != nil {
		t.Fatalf("create test tables: %v", err)
	}

	exists, err := conn.refreshInformationSchemaTable(ctx, "target_users")
	if err != nil {
		t.Fatalf("refresh target table: %v", err)
	}
	if !exists {
		t.Fatal("target table was not found")
	}

	assertInformationSchemaTableNames(t, ctx, conn, "columns", []string{"target_users"})
	assertInformationSchemaTableNames(t, ctx, conn, "statistics", []string{"target_users"})

	if err := conn.refreshInformationSchema(ctx); err != nil {
		t.Fatalf("refresh full information_schema: %v", err)
	}
	assertInformationSchemaTableNames(t, ctx, conn, "columns", []string{"other_users", "target_users"})
}

func TestRefreshInformationSchemaTableSkipsUntilSchemaVersionChanges(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT
);`); err != nil {
		t.Fatalf("create test table: %v", err)
	}

	exists, err := conn.refreshInformationSchemaTable(ctx, "target_users")
	if err != nil {
		t.Fatalf("refresh target table: %v", err)
	}
	if !exists {
		t.Fatal("target table was not found")
	}
	assertInformationSchemaColumnNames(t, ctx, conn, "target_users", []string{"id"})

	if _, err := conn.sqliteConn.ExecContext(ctx, `ALTER TABLE target_users ADD COLUMN cached_out TEXT`); err != nil {
		t.Fatalf("alter test table without version bump: %v", err)
	}
	exists, err = conn.refreshInformationSchemaTable(ctx, "target_users")
	if err != nil {
		t.Fatalf("refresh cached target table: %v", err)
	}
	if !exists {
		t.Fatal("cached target table was not found")
	}
	assertInformationSchemaColumnNames(t, ctx, conn, "target_users", []string{"id"})

	conn.server.bumpSchemaVersion()
	exists, err = conn.refreshInformationSchemaTable(ctx, "target_users")
	if err != nil {
		t.Fatalf("refresh invalidated target table: %v", err)
	}
	if !exists {
		t.Fatal("invalidated target table was not found")
	}
	assertInformationSchemaColumnNames(t, ctx, conn, "target_users", []string{"id", "cached_out"})
}

func TestRefreshInformationSchemaFullSkipsUntilSchemaVersionChanges(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT
);`); err != nil {
		t.Fatalf("create test table: %v", err)
	}

	if err := conn.refreshInformationSchema(ctx); err != nil {
		t.Fatalf("refresh full information_schema: %v", err)
	}
	assertInformationSchemaColumnNames(t, ctx, conn, "target_users", []string{"id"})

	if _, err := conn.sqliteConn.ExecContext(ctx, `ALTER TABLE target_users ADD COLUMN cached_out TEXT`); err != nil {
		t.Fatalf("alter test table without version bump: %v", err)
	}
	if err := conn.refreshInformationSchema(ctx); err != nil {
		t.Fatalf("refresh cached full information_schema: %v", err)
	}
	assertInformationSchemaColumnNames(t, ctx, conn, "target_users", []string{"id"})

	conn.server.bumpSchemaVersion()
	if err := conn.refreshInformationSchema(ctx); err != nil {
		t.Fatalf("refresh invalidated full information_schema: %v", err)
	}
	assertInformationSchemaColumnNames(t, ctx, conn, "target_users", []string{"id", "cached_out"})
}

func TestRefreshInformationSchemaWorksInsideTransaction(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE
);`); err != nil {
		t.Fatalf("create test table: %v", err)
	}
	if _, err := conn.sqliteConn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin outer transaction: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.sqliteConn.ExecContext(ctx, "ROLLBACK")
	})

	if err := conn.refreshInformationSchema(ctx); err != nil {
		t.Fatalf("refresh full information_schema in transaction: %v", err)
	}
	assertInformationSchemaColumnNames(t, ctx, conn, "target_users", []string{"id", "email"})

	if _, err := conn.sqliteConn.ExecContext(ctx, "INSERT INTO target_users (email) VALUES (?)", "tx@example.com"); err != nil {
		t.Fatalf("outer transaction after information_schema refresh: %v", err)
	}
}

func TestExecSQLiteSchemaChangeInvalidatesInformationSchemaCache(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT
);`); err != nil {
		t.Fatalf("create test table: %v", err)
	}
	if _, err := conn.refreshInformationSchemaTable(ctx, "target_users"); err != nil {
		t.Fatalf("refresh target table: %v", err)
	}
	assertInformationSchemaColumnNames(t, ctx, conn, "target_users", []string{"id"})

	if _, err := conn.execSQLite(ctx, `ALTER TABLE target_users ADD COLUMN email TEXT`); err != nil {
		t.Fatalf("alter test table through execSQLite: %v", err)
	}
	if _, err := conn.refreshInformationSchemaTable(ctx, "target_users"); err != nil {
		t.Fatalf("refresh invalidated target table: %v", err)
	}
	assertInformationSchemaColumnNames(t, ctx, conn, "target_users", []string{"id", "email"})
}

func TestInformationSchemaQueryUsesTargetTableRefresh(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE
);
CREATE TABLE other_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL
);`); err != nil {
		t.Fatalf("create test tables: %v", err)
	}

	rs, err := conn.queryInformationSchema(ctx, `
SELECT COLUMN_NAME
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?
ORDER BY ORDINAL_POSITION`, "target_users")
	if err != nil {
		t.Fatalf("query information_schema.columns: %v", err)
	}
	if got, want := resultColumnValues(rs, 0), []any{"id", "email"}; !equalAnySlices(got, want) {
		t.Fatalf("information_schema.columns result = %#v, want %#v", got, want)
	}

	assertInformationSchemaTableNames(t, ctx, conn, "columns", []string{"target_users"})
}

func TestExecuteQueryCOMQueryStreamsInformationSchemaResultSet(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE
);`); err != nil {
		t.Fatalf("create test table: %v", err)
	}

	resp, err := conn.executeQuery(ctx, "COM_QUERY", `
SELECT COLUMN_NAME
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?
ORDER BY ORDINAL_POSITION`, "target_users")
	if err != nil {
		t.Fatalf("execute COM_QUERY information_schema: %v", err)
	}
	stream, ok := resp.(*sqliteResultSet)
	if !ok {
		t.Fatalf("executeQuery() returned %T, want *sqliteResultSet", resp)
	}

	rs, err := stream.materialize()
	if err != nil {
		t.Fatalf("materialize streaming information_schema result: %v", err)
	}
	if got, want := resultColumnValues(rs, 0), []any{"id", "email"}; !equalAnySlices(got, want) {
		t.Fatalf("information_schema.columns result = %#v, want %#v", got, want)
	}
}

func TestExecuteQueryPreparedPathKeepsMaterializedInformationSchemaResultSet(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE
);`); err != nil {
		t.Fatalf("create test table: %v", err)
	}

	resp, err := conn.executeQuery(ctx, "COM_STMT_EXECUTE", `
SELECT COLUMN_NAME
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?
ORDER BY ORDINAL_POSITION`, "target_users")
	if err != nil {
		t.Fatalf("execute COM_STMT_EXECUTE information_schema: %v", err)
	}
	rs, ok := resp.(resultSet)
	if !ok {
		t.Fatalf("executeQuery() returned %T, want resultSet", resp)
	}
	if got, want := resultColumnValues(rs, 0), []any{"id", "email"}; !equalAnySlices(got, want) {
		t.Fatalf("information_schema.columns result = %#v, want %#v", got, want)
	}
}

func TestShowFullFieldsAndShowKeysUseTargetTableRefresh(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE
);
CREATE INDEX idx_target_users_email ON target_users (email);
CREATE TABLE other_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL
);`); err != nil {
		t.Fatalf("create test tables: %v", err)
	}

	fields, err := conn.showFullFields(ctx, "SHOW FULL FIELDS FROM `target_users`")
	if err != nil {
		t.Fatalf("show full fields: %v", err)
	}
	if got, want := resultColumnValues(fields, 0), []any{"id", "email"}; !equalAnySlices(got, want) {
		t.Fatalf("SHOW FULL FIELDS result = %#v, want %#v", got, want)
	}
	assertInformationSchemaTableNames(t, ctx, conn, "columns", []string{"target_users"})

	keys, err := conn.showKeys(ctx, "SHOW KEYS FROM `target_users`")
	if err != nil {
		t.Fatalf("show keys: %v", err)
	}
	if len(keys.Rows) == 0 {
		t.Fatal("SHOW KEYS returned no rows")
	}
	assertInformationSchemaTableNames(t, ctx, conn, "statistics", []string{"target_users"})
}

func newInformationSchemaTestConn(t testing.TB, ctx context.Context) *mysqlConn {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	sqliteConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("open sqlite conn: %v", err)
	}
	t.Cleanup(func() {
		_ = sqliteConn.Close()
	})

	return &mysqlConn{
		sqliteConn:             sqliteConn,
		server:                 &Server{indexMetadata: map[string]mysqlIndexMetadata{}},
		currentDB:              "mysqlmock",
		characterSetConnection: "utf8mb4",
		collationConnection:    "utf8mb4_0900_ai_ci",
	}
}

func assertInformationSchemaTableNames(t *testing.T, ctx context.Context, conn *mysqlConn, table string, want []string) {
	t.Helper()

	rows, err := conn.sqliteConn.QueryContext(ctx, `
SELECT DISTINCT TABLE_NAME
FROM "information_schema".`+quoteIdent(table)+`
ORDER BY TABLE_NAME`)
	if err != nil {
		t.Fatalf("query information_schema.%s table names: %v", table, err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.Fatalf("scan information_schema.%s table name: %v", table, err)
		}
		got = append(got, tableName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read information_schema.%s table names: %v", table, err)
	}
	if !equalStringSlices(got, want) {
		t.Fatalf("information_schema.%s table names = %#v, want %#v", table, got, want)
	}
}

func assertInformationSchemaColumnNames(t *testing.T, ctx context.Context, conn *mysqlConn, tableName string, want []string) {
	t.Helper()

	rows, err := conn.sqliteConn.QueryContext(ctx, `
SELECT COLUMN_NAME
FROM "information_schema"."columns"
WHERE TABLE_NAME = ?
ORDER BY ORDINAL_POSITION`, tableName)
	if err != nil {
		t.Fatalf("query information_schema.columns for %s: %v", tableName, err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			t.Fatalf("scan information_schema.columns for %s: %v", tableName, err)
		}
		got = append(got, columnName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read information_schema.columns for %s: %v", tableName, err)
	}
	if !equalStringSlices(got, want) {
		t.Fatalf("information_schema.columns for %s = %#v, want %#v", tableName, got, want)
	}
}

func resultColumnValues(rs resultSet, index int) []any {
	out := make([]any, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		if index < len(row) {
			out = append(out, row[index])
		}
	}
	return out
}

func equalAnySlices(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func BenchmarkInformationSchemaRefresh(b *testing.B) {
	ctx := context.Background()
	for _, tableCount := range []int{10, 100} {
		b.Run("tables_"+stringForCacheTestInt(tableCount), func(b *testing.B) {
			b.Run("target_table", func(b *testing.B) {
				conn := newInformationSchemaBenchmarkConn(b, ctx, tableCount)
				target := informationSchemaBenchmarkTableName(tableCount - 1)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					conn.server.bumpSchemaVersion()
					exists, err := conn.refreshInformationSchemaTable(ctx, target)
					if err != nil {
						b.Fatalf("refresh target table: %v", err)
					}
					if !exists {
						b.Fatalf("target table %s was not found", target)
					}
				}
				b.StopTimer()

				rows, err := countInformationSchemaRows(ctx, conn, "columns")
				if err != nil {
					b.Fatalf("count information_schema.columns: %v", err)
				}
				benchmarkInformationSchemaRows = rows
			})

			b.Run("full_refresh", func(b *testing.B) {
				conn := newInformationSchemaBenchmarkConn(b, ctx, tableCount)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					conn.server.bumpSchemaVersion()
					if err := conn.refreshInformationSchema(ctx); err != nil {
						b.Fatalf("refresh full information_schema: %v", err)
					}
				}
				b.StopTimer()

				rows, err := countInformationSchemaRows(ctx, conn, "columns")
				if err != nil {
					b.Fatalf("count information_schema.columns: %v", err)
				}
				benchmarkInformationSchemaRows = rows
			})
		})
	}
}

func BenchmarkInformationSchemaQuery(b *testing.B) {
	ctx := context.Background()
	const tableCount = 100
	query := `
SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
FROM information_schema.columns
ORDER BY TABLE_NAME, ORDINAL_POSITION`

	b.Run("tables_100/com_query_streaming", func(b *testing.B) {
		conn := newInformationSchemaBenchmarkConn(b, ctx, tableCount)
		if err := conn.refreshInformationSchema(ctx); err != nil {
			b.Fatalf("warm information_schema: %v", err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := conn.executeQuery(ctx, "COM_QUERY", query)
			if err != nil {
				b.Fatalf("query information_schema stream: %v", err)
			}
			stream, ok := resp.(*sqliteResultSet)
			if !ok {
				b.Fatalf("query information_schema stream returned %T, want *sqliteResultSet", resp)
			}
			rows, err := consumeBenchmarkSQLiteResultStream(stream)
			if err != nil {
				b.Fatalf("consume information_schema stream: %v", err)
			}
			benchmarkInformationSchemaRows = rows
		}
	})

	b.Run("tables_100/prepared_materialized", func(b *testing.B) {
		conn := newInformationSchemaBenchmarkConn(b, ctx, tableCount)
		if err := conn.refreshInformationSchema(ctx); err != nil {
			b.Fatalf("warm information_schema: %v", err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := conn.executeQuery(ctx, "COM_STMT_EXECUTE", query)
			if err != nil {
				b.Fatalf("query information_schema materialized: %v", err)
			}
			rs, ok := resp.(resultSet)
			if !ok {
				b.Fatalf("query information_schema materialized returned %T, want resultSet", resp)
			}
			benchmarkInformationSchemaRows = len(rs.Rows)
		}
	})
}

func newInformationSchemaBenchmarkConn(b *testing.B, ctx context.Context, tableCount int) *mysqlConn {
	b.Helper()

	conn := newInformationSchemaTestConn(b, ctx)
	if err := setupInformationSchemaBenchmarkTables(ctx, conn, tableCount); err != nil {
		b.Fatalf("setup benchmark tables: %v", err)
	}
	return conn
}

func setupInformationSchemaBenchmarkTables(ctx context.Context, conn *mysqlConn, tableCount int) error {
	for i := 0; i < tableCount; i++ {
		tableName := informationSchemaBenchmarkTableName(i)
		createSQL := fmt.Sprintf(`
CREATE TABLE %s (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id INTEGER NOT NULL,
  email TEXT NOT NULL UNIQUE,
  name TEXT,
  created_at TEXT,
  active INTEGER DEFAULT 1
)`, quoteIdent(tableName))
		if _, err := conn.sqliteConn.ExecContext(ctx, createSQL); err != nil {
			return err
		}
		if _, err := conn.sqliteConn.ExecContext(
			ctx,
			fmt.Sprintf("CREATE INDEX %s ON %s (account_id, created_at)", quoteIdent("idx_"+tableName+"_account_created"), quoteIdent(tableName)),
		); err != nil {
			return err
		}
	}
	return nil
}

func informationSchemaBenchmarkTableName(index int) string {
	return "info_bench_" + stringForCacheTestInt(index)
}

func countInformationSchemaRows(ctx context.Context, conn *mysqlConn, table string) (int, error) {
	var count int
	err := conn.sqliteConn.QueryRowContext(ctx, `SELECT COUNT(*) FROM "information_schema".`+quoteIdent(table)).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
