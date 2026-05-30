package mysqlmock

import (
	"context"
	"testing"
)

func TestSQLiteTableUsesAutoincrementCachesBySchemaVersion(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE sequence_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT
)`); err != nil {
		t.Fatalf("create sequence table: %v", err)
	}

	uses, err := conn.sqliteTableUsesAutoincrement(ctx, "sequence_users")
	if err != nil {
		t.Fatalf("lookup autoincrement: %v", err)
	}
	if !uses {
		t.Fatal("sequence_users should use AUTOINCREMENT")
	}
	if got := phaseTimingCount(conn, "mysql.auto_increment.sqlite_table_lookup"); got != 1 {
		t.Fatalf("sqlite_table_lookup count = %d, want 1", got)
	}

	uses, err = conn.sqliteTableUsesAutoincrement(ctx, "sequence_users")
	if err != nil {
		t.Fatalf("lookup cached autoincrement: %v", err)
	}
	if !uses {
		t.Fatal("cached sequence_users should use AUTOINCREMENT")
	}
	if got := phaseTimingCount(conn, "mysql.auto_increment.sqlite_table_lookup"); got != 1 {
		t.Fatalf("sqlite_table_lookup count after cache hit = %d, want 1", got)
	}

	conn.server.bumpSchemaVersion()
	uses, err = conn.sqliteTableUsesAutoincrement(ctx, "sequence_users")
	if err != nil {
		t.Fatalf("lookup invalidated autoincrement: %v", err)
	}
	if !uses {
		t.Fatal("invalidated sequence_users should use AUTOINCREMENT")
	}
	if got := phaseTimingCount(conn, "mysql.auto_increment.sqlite_table_lookup"); got != 2 {
		t.Fatalf("sqlite_table_lookup count after schema change = %d, want 2", got)
	}
}

func TestRollbackRestoresOnlyDirtyAutoIncrementTables(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE committed_sequence (
  id INTEGER PRIMARY KEY AUTOINCREMENT
);
CREATE TABLE rolled_back_sequence (
  id INTEGER PRIMARY KEY AUTOINCREMENT
)`); err != nil {
		t.Fatalf("create sequence tables: %v", err)
	}

	if _, err := conn.executeQuery(ctx, "COM_QUERY", "INSERT INTO committed_sequence DEFAULT VALUES"); err != nil {
		t.Fatalf("insert committed sequence row: %v", err)
	}
	if _, err := conn.executeQuery(ctx, "COM_QUERY", "BEGIN"); err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	rolledBack, err := conn.executeQuery(ctx, "COM_QUERY", "INSERT INTO rolled_back_sequence DEFAULT VALUES")
	if err != nil {
		t.Fatalf("insert rolled back sequence row: %v", err)
	}
	if result, ok := rolledBack.(okResult); !ok || result.LastInsertID != 1 {
		t.Fatalf("rolled back insert result = %#v, want last insert id 1", rolledBack)
	}
	if _, err := conn.executeQuery(ctx, "COM_QUERY", "ROLLBACK"); err != nil {
		t.Fatalf("rollback transaction: %v", err)
	}

	if got := phaseTimingCount(conn, "mysql.auto_increment.sequence_update"); got != 1 {
		t.Fatalf("sequence_update count = %d, want only the rolled back table", got)
	}
	if len(conn.autoIncrementRestoreTables) != 0 {
		t.Fatalf("restore tables after rollback = %#v, want cleared", conn.autoIncrementRestoreTables)
	}

	inserted, err := conn.executeQuery(ctx, "COM_QUERY", "INSERT INTO rolled_back_sequence DEFAULT VALUES")
	if err != nil {
		t.Fatalf("insert after rollback: %v", err)
	}
	if result, ok := inserted.(okResult); !ok || result.LastInsertID != 2 {
		t.Fatalf("insert after rollback result = %#v, want last insert id 2", inserted)
	}
}

func phaseTimingCount(conn *mysqlConn, phase string) uint64 {
	return conn.server.Stats().Timings.Phases.ByPhase[phase].Count
}
