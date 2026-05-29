package mysqlmock

import (
	"context"
	"testing"
	"time"
)

var benchmarkMySQLDMLResult okResult

func TestMySQLUpsertPlaceholderValuesDoNotMutateArgs(t *testing.T) {
	ctx := context.Background()
	conn, cleanup := newMySQLDMLBenchmarkConn(t, ctx)
	defer cleanup()

	sqlText := `
INSERT INTO dml_bench_users (email, name, login_count, updated_at)
VALUES ( ? , ? , ? , ? )
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  login_count = login_count + VALUES(login_count),
  updated_at = VALUES(updated_at)
`
	args := []any{"existing@example.com", "Updated", 2, "2026-05-28 11:00:00"}
	result, handled, err := conn.execMySQLUpsert(ctx, sqlText, args...)
	if err != nil {
		t.Fatalf("exec ON DUPLICATE KEY UPDATE: %v", err)
	}
	if !handled {
		t.Fatal("ON DUPLICATE KEY UPDATE was not handled")
	}
	if result.AffectedRows != 2 {
		t.Fatalf("affected rows = %d, want 2", result.AffectedRows)
	}
	if args[0] != "existing@example.com" || args[1] != "Updated" || args[2] != 2 || args[3] != "2026-05-28 11:00:00" {
		t.Fatalf("args were mutated: %#v", args)
	}

	rs, err := conn.querySQLite(ctx, "SELECT name, login_count, updated_at FROM dml_bench_users WHERE email = ?", "existing@example.com")
	if err != nil {
		t.Fatalf("select upserted row: %v", err)
	}
	if len(rs.Rows) != 1 || len(rs.Rows[0]) != 3 {
		t.Fatalf("upserted rows = %#v, want one row", rs.Rows)
	}
	updatedAt, ok := rs.Rows[0][2].(time.Time)
	if !ok {
		t.Fatalf("updated_at = %T(%v), want time.Time", rs.Rows[0][2], rs.Rows[0][2])
	}
	if rs.Rows[0][0] != "Updated" || rs.Rows[0][1] != int64(3) || !updatedAt.Equal(time.Date(2026, time.May, 28, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("upserted row = %#v, want updated values", rs.Rows[0])
	}
}

func BenchmarkMySQLCompatibleDML(b *testing.B) {
	ctx := context.Background()

	b.Run("on_duplicate_key_update/duplicate", func(b *testing.B) {
		conn, cleanup := newMySQLDMLBenchmarkConn(b, ctx)
		defer cleanup()

		sqlText := `
INSERT INTO dml_bench_users (email, name, login_count, updated_at)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  login_count = login_count + VALUES(login_count),
  updated_at = VALUES(updated_at)
`
		args := []any{"existing@example.com", "Updated", 1, "2026-05-28 11:00:00"}
		warmMySQLDMLUpsert(b, ctx, conn, sqlText, args)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, handled, err := conn.execMySQLUpsert(ctx, sqlText, args...)
			if err != nil {
				b.Fatalf("exec ON DUPLICATE KEY UPDATE: %v", err)
			}
			if !handled {
				b.Fatal("ON DUPLICATE KEY UPDATE was not handled")
			}
			benchmarkMySQLDMLResult = result
		}
	})

	b.Run("insert_ignore/duplicate", func(b *testing.B) {
		conn, cleanup := newMySQLDMLBenchmarkConn(b, ctx)
		defer cleanup()

		sqlText := `
INSERT IGNORE INTO dml_bench_users (email, name, login_count, updated_at)
VALUES (?, ?, ?, ?)
`
		args := []any{"existing@example.com", "Ignored", 99, "2026-05-28 11:00:00"}
		warmMySQLDMLInsertCompatibility(b, ctx, conn, sqlText, args)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, handled, err := conn.execMySQLInsertCompatibility(ctx, sqlText, args...)
			if err != nil {
				b.Fatalf("exec INSERT IGNORE: %v", err)
			}
			if !handled {
				b.Fatal("INSERT IGNORE was not handled")
			}
			benchmarkMySQLDMLResult = result
		}
	})

	b.Run("replace/duplicate", func(b *testing.B) {
		conn, cleanup := newMySQLDMLBenchmarkConn(b, ctx)
		defer cleanup()

		sqlText := `
REPLACE INTO dml_bench_users (email, name, login_count, updated_at)
VALUES (?, ?, ?, ?)
`
		args := []any{"existing@example.com", "Replaced", 3, "2026-05-28 11:00:00"}
		warmMySQLDMLInsertCompatibility(b, ctx, conn, sqlText, args)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, handled, err := conn.execMySQLInsertCompatibility(ctx, sqlText, args...)
			if err != nil {
				b.Fatalf("exec REPLACE: %v", err)
			}
			if !handled {
				b.Fatal("REPLACE was not handled")
			}
			benchmarkMySQLDMLResult = result
		}
	})
}

func newMySQLDMLBenchmarkConn(b testing.TB, ctx context.Context) (*mysqlConn, func()) {
	b.Helper()

	cfg := DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE dml_bench_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  login_count INTEGER NOT NULL DEFAULT 0,
  updated_at DATETIME NULL
);`}
	cfg.Seed = map[string][]map[string]any{
		"dml_bench_users": {
			{
				"email":       "existing@example.com",
				"name":        "Existing",
				"login_count": 1,
				"updated_at":  "2026-05-28 10:00:00",
			},
		},
	}
	server, err := New(WithConfig(cfg))
	if err != nil {
		b.Fatalf("new server: %v", err)
	}
	if err := server.openBackend(ctx); err != nil {
		b.Fatalf("open backend: %v", err)
	}
	sqliteConn, err := server.db.Conn(ctx)
	if err != nil {
		_ = server.closeBackend()
		b.Fatalf("sqlite conn: %v", err)
	}
	if _, err := sqliteConn.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		_ = sqliteConn.Close()
		_ = server.closeBackend()
		b.Fatalf("sqlite conn init: %v", err)
	}

	conn := &mysqlConn{
		sqliteConn:  sqliteConn,
		server:      server,
		statusFlags: serverStatusAutocommit,
		currentDB:   "mysqlmock",
	}
	cleanup := func() {
		_ = sqliteConn.Close()
		_ = server.closeBackend()
	}
	return conn, cleanup
}

func warmMySQLDMLUpsert(b *testing.B, ctx context.Context, conn *mysqlConn, sqlText string, args []any) {
	b.Helper()

	if result, handled, err := conn.execMySQLUpsert(ctx, sqlText, args...); err != nil {
		b.Fatalf("warm ON DUPLICATE KEY UPDATE: %v", err)
	} else if !handled {
		b.Fatal("warm ON DUPLICATE KEY UPDATE was not handled")
	} else {
		benchmarkMySQLDMLResult = result
	}
}

func warmMySQLDMLInsertCompatibility(b *testing.B, ctx context.Context, conn *mysqlConn, sqlText string, args []any) {
	b.Helper()

	if result, handled, err := conn.execMySQLInsertCompatibility(ctx, sqlText, args...); err != nil {
		b.Fatalf("warm INSERT compatibility: %v", err)
	} else if !handled {
		b.Fatal("warm INSERT compatibility was not handled")
	} else {
		benchmarkMySQLDMLResult = result
	}
}
