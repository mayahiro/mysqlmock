package mysqlmock

import (
	"testing"
	"time"
)

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
	store.recordInformationSchemaQuery(true, "")
	store.recordInformationSchemaQuery(false, informationSchemaBroadReasonNoTableNameFilter)
	store.recordShowFullFieldsQuery()
	store.recordInformationSchemaTargetTableRefresh(true)
	store.recordReset("data_only")
	store.recordSchemaChange()
	store.recordQueryTiming("COM_QUERY", "sqlite", "SELECT 1", 10*time.Millisecond)
	store.recordQueryTiming("COM_STMT_EXECUTE", "compat", "SHOW FULL FIELDS FROM users", 20*time.Millisecond)
	store.recordPhaseTiming("sqlite.exec", 5*time.Millisecond)

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
	if stats.Metadata.InformationSchemaQueries != 2 ||
		stats.Metadata.TargetedInformationSchemaQueries != 1 ||
		stats.Metadata.BroadInformationSchemaQueries != 1 ||
		stats.Metadata.BroadInformationSchemaQueryReasons[informationSchemaBroadReasonNoTableNameFilter] != 1 ||
		stats.Metadata.ShowFullFieldsQueries != 1 ||
		stats.Metadata.TargetTableRefreshes != 1 ||
		stats.Metadata.TablesLoaded != 0 {
		t.Fatalf("metadata stats = %#v, want targeted information_schema and show counts", stats.Metadata)
	}
	if stats.Resets.Total != 1 || stats.Resets.DataOnly != 1 || stats.Resets.Full != 0 {
		t.Fatalf("reset stats = %#v, want one data-only reset", stats.Resets)
	}
	if stats.Timings.Queries.Count != 2 ||
		stats.Timings.Queries.TotalNanos != uint64(30*time.Millisecond) ||
		stats.Timings.Queries.MaxNanos != uint64(20*time.Millisecond) {
		t.Fatalf("query timing stats = %#v, want two query durations", stats.Timings.Queries)
	}
	if stats.Timings.Queries.ByRoute["sqlite"].TotalNanos != uint64(10*time.Millisecond) ||
		stats.Timings.Queries.ByKind["show_full_fields"].MaxNanos != uint64(20*time.Millisecond) {
		t.Fatalf("query timing buckets = %#v, want route and kind durations", stats.Timings.Queries)
	}
	if stats.Timings.Phases.Count != 1 ||
		stats.Timings.Phases.ByPhase["sqlite.exec"].TotalNanos != uint64(5*time.Millisecond) {
		t.Fatalf("phase timing stats = %#v, want sqlite.exec duration", stats.Timings.Phases)
	}

	stats.Queries.ByCommand["COM_QUERY"] = 99
	stats.Queries.ByRoute["sqlite"] = 99
	stats.Queries.ByKind["select"] = 99
	stats.Timings.Queries.ByRoute["sqlite"] = TimingBucket{Count: 99, TotalNanos: 99, MaxNanos: 99}
	stats.Timings.Phases.ByPhase["sqlite.exec"] = TimingBucket{Count: 99, TotalNanos: 99, MaxNanos: 99}
	stats.Metadata.BroadInformationSchemaQueryReasons[informationSchemaBroadReasonNoTableNameFilter] = 99
	again := store.snapshot()
	if again.Queries.ByCommand["COM_QUERY"] != 1 ||
		again.Queries.ByRoute["sqlite"] != 1 ||
		again.Queries.ByKind["select"] != 1 {
		t.Fatalf("stats snapshot mutation changed store: %#v", again.Queries)
	}
	if again.Timings.Queries.ByRoute["sqlite"].Count != 1 ||
		again.Timings.Phases.ByPhase["sqlite.exec"].Count != 1 {
		t.Fatalf("timing snapshot mutation changed store: %#v", again.Timings)
	}
	if again.Metadata.BroadInformationSchemaQueryReasons[informationSchemaBroadReasonNoTableNameFilter] != 1 {
		t.Fatalf("metadata snapshot mutation changed store: %#v", again.Metadata)
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
