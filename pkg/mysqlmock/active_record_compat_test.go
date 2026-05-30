package mysqlmock

import (
	"context"
	"testing"
)

func TestParseActiveRecordShowStatements(t *testing.T) {
	if !isShowFullFieldsQuery("SHOW FULL FIELDS FROM `AR_USERS`") {
		t.Fatal("SHOW FULL FIELDS was not recognized")
	}
	table, like, ok := parseShowFullFields("SHOW FULL FIELDS FROM `ar_users` LIKE 'email'")
	if !ok || table != "ar_users" || like != "email" {
		t.Fatalf("parse SHOW FULL FIELDS = table:%q like:%q ok:%v, want ar_users/email/true", table, like, ok)
	}
	table, ok = parseShowCreateTable("SHOW CREATE TABLE `ar_users`")
	if !ok || table != "ar_users" {
		t.Fatalf("parse SHOW CREATE TABLE = table:%q ok:%v, want ar_users/true", table, ok)
	}
	table, ok = parseShowKeys("SHOW KEYS FROM `ar_users`")
	if !ok || table != "ar_users" {
		t.Fatalf("parse SHOW KEYS = table:%q ok:%v, want ar_users/true", table, ok)
	}
}

func TestShowFullFieldsUsesCachedResultUntilSchemaVersionChanges(t *testing.T) {
	ctx := context.Background()
	conn := newInformationSchemaTestConn(t, ctx)

	if _, err := conn.sqliteConn.ExecContext(ctx, `
CREATE TABLE target_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE
)`); err != nil {
		t.Fatalf("create target_users: %v", err)
	}

	first, err := conn.showFullFields(ctx, "SHOW FULL FIELDS FROM target_users")
	if err != nil {
		t.Fatalf("show full fields: %v", err)
	}
	if got, want := resultColumnValues(first, 0), []any{"id", "email"}; !equalAnySlices(got, want) {
		t.Fatalf("SHOW FULL FIELDS columns = %#v, want %#v", got, want)
	}
	sqliteQueries := phaseTimingCount(conn, "sqlite.query")
	targetRefreshes := conn.server.Stats().Metadata.TargetTableRefreshes

	second, err := conn.showFullFields(ctx, "SHOW FULL FIELDS FROM target_users")
	if err != nil {
		t.Fatalf("show full fields cached: %v", err)
	}
	if got, want := resultColumnValues(second, 0), []any{"id", "email"}; !equalAnySlices(got, want) {
		t.Fatalf("cached SHOW FULL FIELDS columns = %#v, want %#v", got, want)
	}
	if got := phaseTimingCount(conn, "sqlite.query"); got != sqliteQueries {
		t.Fatalf("sqlite.query count after cache hit = %d, want %d", got, sqliteQueries)
	}
	if got := conn.server.Stats().Metadata.TargetTableRefreshes; got != targetRefreshes {
		t.Fatalf("target table refreshes after cache hit = %d, want %d", got, targetRefreshes)
	}

	second.Rows[0][0] = "mutated"
	third, err := conn.showFullFields(ctx, "SHOW FULL FIELDS FROM target_users")
	if err != nil {
		t.Fatalf("show full fields cached after mutation: %v", err)
	}
	if got, want := resultColumnValues(third, 0), []any{"id", "email"}; !equalAnySlices(got, want) {
		t.Fatalf("cached SHOW FULL FIELDS after mutation = %#v, want %#v", got, want)
	}

	if _, err := conn.sqliteConn.ExecContext(ctx, `ALTER TABLE target_users ADD COLUMN cached_out TEXT`); err != nil {
		t.Fatalf("alter target_users without schema bump: %v", err)
	}
	stale, err := conn.showFullFields(ctx, "SHOW FULL FIELDS FROM target_users")
	if err != nil {
		t.Fatalf("show full fields before schema bump: %v", err)
	}
	if got, want := resultColumnValues(stale, 0), []any{"id", "email"}; !equalAnySlices(got, want) {
		t.Fatalf("SHOW FULL FIELDS before schema bump = %#v, want %#v", got, want)
	}

	conn.server.bumpSchemaVersion()
	refreshed, err := conn.showFullFields(ctx, "SHOW FULL FIELDS FROM target_users")
	if err != nil {
		t.Fatalf("show full fields after schema bump: %v", err)
	}
	if got, want := resultColumnValues(refreshed, 0), []any{"id", "email", "cached_out"}; !equalAnySlices(got, want) {
		t.Fatalf("SHOW FULL FIELDS after schema bump = %#v, want %#v", got, want)
	}
}
