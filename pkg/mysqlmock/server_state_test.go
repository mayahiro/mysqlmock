package mysqlmock

import "testing"

func TestDiagnosticsStoreSnapshotsAreCopies(t *testing.T) {
	var store diagnosticsStore
	store.recordUnsupported(UnsupportedQuery{SQL: "CREATE USER example"})
	store.recordQuery(QueryEvent{SQL: "SELECT 1"})

	unsupported := store.unsupportedSnapshot()
	queries := store.queriesSnapshot()
	if len(unsupported) != 1 {
		t.Fatalf("unsupported snapshot count = %d, want 1", len(unsupported))
	}
	if len(queries) != 1 {
		t.Fatalf("query snapshot count = %d, want 1", len(queries))
	}

	unsupported[0].SQL = "mutated"
	queries[0].SQL = "mutated"

	if got := store.unsupportedSnapshot()[0].SQL; got != "CREATE USER example" {
		t.Fatalf("stored unsupported SQL = %q, want original value", got)
	}
	if got := store.queriesSnapshot()[0].SQL; got != "SELECT 1" {
		t.Fatalf("stored query SQL = %q, want original value", got)
	}

	store.reset()
	if got := len(store.unsupportedSnapshot()); got != 0 {
		t.Fatalf("unsupported snapshot count after reset = %d, want 0", got)
	}
	if got := len(store.queriesSnapshot()); got != 0 {
		t.Fatalf("query snapshot count after reset = %d, want 0", got)
	}
}

func TestStatsStoreSnapshotsAreCopies(t *testing.T) {
	var store statsStore
	store.recordQuery("COM_QUERY", "sqlite", "SELECT 1")
	store.recordQuery("COM_STMT_EXECUTE", "compat", "SHOW FULL FIELDS FROM users")
	store.recordUnsupported()
	store.recordInformationSchemaQuery(true)
	store.recordShowFullFieldsQuery()
	store.recordInformationSchemaTargetTableRefresh(true)
	store.recordReset("data_only")
	store.recordSchemaChange()

	stats := store.snapshot()
	if stats.Queries.Total != 2 {
		t.Fatalf("query total = %d, want 2", stats.Queries.Total)
	}
	if stats.Queries.ByCommand["COM_QUERY"] != 1 || stats.Queries.ByCommand["COM_STMT_EXECUTE"] != 1 {
		t.Fatalf("query commands = %#v, want COM_QUERY and COM_STMT_EXECUTE counts", stats.Queries.ByCommand)
	}
	if stats.Queries.ByRoute["sqlite"] != 1 || stats.Queries.ByRoute["compat"] != 1 {
		t.Fatalf("query routes = %#v, want sqlite and compat counts", stats.Queries.ByRoute)
	}
	if stats.Queries.ByKind["select"] != 1 || stats.Queries.ByKind["show_full_fields"] != 1 {
		t.Fatalf("query kinds = %#v, want select and show_full_fields counts", stats.Queries.ByKind)
	}
	if stats.Unsupported != 1 || stats.SchemaChanges != 1 {
		t.Fatalf("unsupported/schema changes = %d/%d, want 1/1", stats.Unsupported, stats.SchemaChanges)
	}
	if stats.Metadata.InformationSchemaQueries != 1 ||
		stats.Metadata.TargetedInformationSchemaQueries != 1 ||
		stats.Metadata.ShowFullFieldsQueries != 1 ||
		stats.Metadata.TargetTableRefreshes != 1 ||
		stats.Metadata.TablesLoaded != 0 {
		t.Fatalf("metadata stats = %#v, want targeted information_schema and show counts", stats.Metadata)
	}
	if stats.Resets.Total != 1 || stats.Resets.DataOnly != 1 || stats.Resets.Full != 0 {
		t.Fatalf("reset stats = %#v, want one data-only reset", stats.Resets)
	}

	stats.Queries.ByCommand["COM_QUERY"] = 99
	stats.Queries.ByRoute["sqlite"] = 99
	stats.Queries.ByKind["select"] = 99
	again := store.snapshot()
	if again.Queries.ByCommand["COM_QUERY"] != 1 ||
		again.Queries.ByRoute["sqlite"] != 1 ||
		again.Queries.ByKind["select"] != 1 {
		t.Fatalf("stats snapshot mutation changed store: %#v", again.Queries)
	}
}

func TestAdvisoryLockStoreTracksOwnership(t *testing.T) {
	var store advisoryLockStore

	if got := store.get("schema_migrations", 1); got != 1 {
		t.Fatalf("first get = %d, want 1", got)
	}
	if got := store.get("schema_migrations", 1); got != 1 {
		t.Fatalf("same owner get = %d, want 1", got)
	}
	if got := store.get("schema_migrations", 2); got != 0 {
		t.Fatalf("other owner get = %d, want 0", got)
	}
	if got := store.release("schema_migrations", 2); got != 0 {
		t.Fatalf("other owner release = %v, want 0", got)
	}
	if got := store.release("unknown", 1); got != nil {
		t.Fatalf("unknown release = %v, want nil", got)
	}
	if got := store.release("schema_migrations", 1); got != 1 {
		t.Fatalf("owner release = %v, want 1", got)
	}
	if got := store.get("schema_migrations", 2); got != 1 {
		t.Fatalf("get after release = %d, want 1", got)
	}

	store.get("other", 2)
	store.releaseAll(2)
	if got := store.release("schema_migrations", 2); got != nil {
		t.Fatalf("released held lock = %v, want nil", got)
	}
	if got := store.release("other", 2); got != nil {
		t.Fatalf("released other lock = %v, want nil", got)
	}
}
