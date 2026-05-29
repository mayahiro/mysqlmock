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
