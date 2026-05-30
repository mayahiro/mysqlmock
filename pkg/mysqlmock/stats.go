package mysqlmock

import (
	"strings"
	"sync"
	"time"
)

// Stats is a SQL-body-free snapshot of server execution counters.
type Stats struct {
	Queries       QueryStats    `json:"queries"`
	Metadata      MetadataStats `json:"metadata"`
	Resets        ResetStats    `json:"resets"`
	Timings       TimingStats   `json:"timings"`
	SchemaChanges uint64        `json:"schema_changes"`
	Unsupported   uint64        `json:"unsupported"`
}

// QueryStats summarizes routed SQL without storing SQL text or parameters.
type QueryStats struct {
	Total     uint64            `json:"total"`
	ByCommand map[string]uint64 `json:"by_command"`
	ByRoute   map[string]uint64 `json:"by_route"`
	ByKind    map[string]uint64 `json:"by_kind"`
}

// MetadataStats summarizes schema metadata work without storing object names.
type MetadataStats struct {
	InformationSchemaQueries           uint64            `json:"information_schema_queries"`
	TargetedInformationSchemaQueries   uint64            `json:"targeted_information_schema_queries"`
	BroadInformationSchemaQueries      uint64            `json:"broad_information_schema_queries"`
	BroadInformationSchemaQueryReasons map[string]uint64 `json:"broad_information_schema_query_reasons,omitempty"`
	ShowFullFieldsQueries              uint64            `json:"show_full_fields_queries"`
	ShowCreateTableQueries             uint64            `json:"show_create_table_queries"`
	ShowKeysQueries                    uint64            `json:"show_keys_queries"`
	TargetTableRefreshes               uint64            `json:"target_table_refreshes"`
	TargetTableCacheHits               uint64            `json:"target_table_cache_hits"`
	TargetTableMisses                  uint64            `json:"target_table_misses"`
	FullRefreshes                      uint64            `json:"full_refreshes"`
	FullRefreshCacheHits               uint64            `json:"full_refresh_cache_hits"`
	TablesLoaded                       uint64            `json:"tables_loaded"`
}

// ResetStats summarizes successful Server.Reset calls.
type ResetStats struct {
	Total    uint64 `json:"total"`
	DataOnly uint64 `json:"data_only"`
	Full     uint64 `json:"full"`
}

// TimingStats summarizes elapsed time without storing SQL text or object names.
type TimingStats struct {
	Queries QueryTimingStats `json:"queries"`
	Phases  PhaseTimingStats `json:"phases"`
}

// QueryTimingStats summarizes query execution durations by SQL-safe categories.
type QueryTimingStats struct {
	Count      uint64                  `json:"count"`
	TotalNanos uint64                  `json:"total_ns"`
	MaxNanos   uint64                  `json:"max_ns"`
	ByCommand  map[string]TimingBucket `json:"by_command"`
	ByRoute    map[string]TimingBucket `json:"by_route"`
	ByKind     map[string]TimingBucket `json:"by_kind"`
}

// PhaseTimingStats summarizes internal phase durations by fixed phase name.
type PhaseTimingStats struct {
	Count      uint64                  `json:"count"`
	TotalNanos uint64                  `json:"total_ns"`
	MaxNanos   uint64                  `json:"max_ns"`
	ByPhase    map[string]TimingBucket `json:"by_phase"`
}

// TimingBucket stores aggregate duration values in nanoseconds.
type TimingBucket struct {
	Count      uint64 `json:"count"`
	TotalNanos uint64 `json:"total_ns"`
	MaxNanos   uint64 `json:"max_ns"`
}

type statsStore struct {
	mu            sync.Mutex
	queries       QueryStats
	metadata      MetadataStats
	resets        ResetStats
	timings       TimingStats
	schemaChanges uint64
	unsupported   uint64
}

func (s *statsStore) snapshot() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{
		Queries: QueryStats{
			Total:     s.queries.Total,
			ByCommand: cloneCountMap(s.queries.ByCommand),
			ByRoute:   cloneCountMap(s.queries.ByRoute),
			ByKind:    cloneCountMap(s.queries.ByKind),
		},
		Metadata: cloneMetadataStats(s.metadata),
		Resets:   s.resets,
		Timings: TimingStats{
			Queries: QueryTimingStats{
				Count:      s.timings.Queries.Count,
				TotalNanos: s.timings.Queries.TotalNanos,
				MaxNanos:   s.timings.Queries.MaxNanos,
				ByCommand:  cloneTimingBucketMap(s.timings.Queries.ByCommand),
				ByRoute:    cloneTimingBucketMap(s.timings.Queries.ByRoute),
				ByKind:     cloneTimingBucketMap(s.timings.Queries.ByKind),
			},
			Phases: PhaseTimingStats{
				Count:      s.timings.Phases.Count,
				TotalNanos: s.timings.Phases.TotalNanos,
				MaxNanos:   s.timings.Phases.MaxNanos,
				ByPhase:    cloneTimingBucketMap(s.timings.Phases.ByPhase),
			},
		},
		SchemaChanges: s.schemaChanges,
		Unsupported:   s.unsupported,
	}
}

func (s *statsStore) recordQuery(command, route, normalizedSQL string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.queries.Total++
	incrementCount(&s.queries.ByCommand, command)
	incrementCount(&s.queries.ByRoute, route)
	incrementCount(&s.queries.ByKind, queryKindFromNormalizedSQL(normalizedSQL))
}

func (s *statsStore) recordQueryTiming(command, route, normalizedSQL string, duration time.Duration) {
	nanos := durationNanos(duration)

	s.mu.Lock()
	defer s.mu.Unlock()

	recordTimingBucket(&s.timings.Queries.Count, &s.timings.Queries.TotalNanos, &s.timings.Queries.MaxNanos, nanos)
	incrementTimingBucket(&s.timings.Queries.ByCommand, command, nanos)
	incrementTimingBucket(&s.timings.Queries.ByRoute, route, nanos)
	incrementTimingBucket(&s.timings.Queries.ByKind, queryKindFromNormalizedSQL(normalizedSQL), nanos)
}

func (s *statsStore) recordPhaseTiming(phase string, duration time.Duration) {
	if phase == "" {
		return
	}
	nanos := durationNanos(duration)

	s.mu.Lock()
	defer s.mu.Unlock()

	recordTimingBucket(&s.timings.Phases.Count, &s.timings.Phases.TotalNanos, &s.timings.Phases.MaxNanos, nanos)
	incrementTimingBucket(&s.timings.Phases.ByPhase, phase, nanos)
}

func (s *statsStore) recordUnsupported() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.unsupported++
}

func (s *statsStore) recordReset(kind string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.resets.Total++
	switch kind {
	case "data_only":
		s.resets.DataOnly++
	case "full":
		s.resets.Full++
	}
}

func (s *statsStore) recordSchemaChange() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.schemaChanges++
}

func (s *statsStore) recordInformationSchemaQuery(targeted bool, broadReason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.InformationSchemaQueries++
	if targeted {
		s.metadata.TargetedInformationSchemaQueries++
	} else {
		s.metadata.BroadInformationSchemaQueries++
		incrementCount(&s.metadata.BroadInformationSchemaQueryReasons, broadReason)
	}
}

func (s *statsStore) recordShowFullFieldsQuery() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.ShowFullFieldsQueries++
}

func (s *statsStore) recordShowCreateTableQuery() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.ShowCreateTableQueries++
}

func (s *statsStore) recordShowKeysQuery() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.ShowKeysQueries++
}

func (s *statsStore) recordInformationSchemaTargetTableCacheHit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.TargetTableCacheHits++
}

func (s *statsStore) recordInformationSchemaTargetTableRefresh(exists bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.TargetTableRefreshes++
	if !exists {
		s.metadata.TargetTableMisses++
	}
}

func (s *statsStore) recordInformationSchemaFullRefreshCacheHit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.FullRefreshCacheHits++
}

func (s *statsStore) recordInformationSchemaFullRefresh() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.FullRefreshes++
}

func (s *statsStore) recordInformationSchemaTableLoaded() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.TablesLoaded++
}

func incrementCount(counts *map[string]uint64, key string) {
	if key == "" {
		return
	}
	if *counts == nil {
		*counts = map[string]uint64{}
	}
	(*counts)[key]++
}

func cloneCountMap(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneMetadataStats(in MetadataStats) MetadataStats {
	out := in
	out.BroadInformationSchemaQueryReasons = cloneCountMap(in.BroadInformationSchemaQueryReasons)
	return out
}

func incrementTimingBucket(counts *map[string]TimingBucket, key string, nanos uint64) {
	if key == "" {
		return
	}
	if *counts == nil {
		*counts = map[string]TimingBucket{}
	}
	bucket := (*counts)[key]
	recordTimingBucket(&bucket.Count, &bucket.TotalNanos, &bucket.MaxNanos, nanos)
	(*counts)[key] = bucket
}

func recordTimingBucket(count, total, max *uint64, nanos uint64) {
	(*count)++
	(*total) += nanos
	if nanos > *max {
		*max = nanos
	}
}

func cloneTimingBucketMap(in map[string]TimingBucket) map[string]TimingBucket {
	out := make(map[string]TimingBucket, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func durationNanos(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64(duration)
}

func queryKindFromNormalizedSQL(sqlText string) string {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return "empty"
	}

	word, _, ok := readSQLIdentifier(sqlText, 0)
	if !ok {
		return "other"
	}

	switch strings.ToUpper(word) {
	case "SHOW":
		switch {
		case hasPrefixFold(sqlText, "SHOW FULL FIELDS ") ||
			hasPrefixFold(sqlText, "SHOW FULL COLUMNS ") ||
			hasPrefixFold(sqlText, "SHOW FIELDS ") ||
			hasPrefixFold(sqlText, "SHOW COLUMNS "):
			return "show_full_fields"
		case hasPrefixFold(sqlText, "SHOW CREATE TABLE "):
			return "show_create_table"
		case hasPrefixFold(sqlText, "SHOW KEYS ") ||
			hasPrefixFold(sqlText, "SHOW INDEX ") ||
			hasPrefixFold(sqlText, "SHOW INDEXES "):
			return "show_keys"
		default:
			return "show"
		}
	case "SET":
		return "set"
	case "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT":
		return "transaction"
	case "START", "RELEASE":
		if isTransactionKind(sqlText) {
			return "transaction"
		}
	case "SELECT":
		if containsFold(sqlText, "information_schema") {
			return "information_schema"
		}
		return "select"
	case "WITH":
		if containsFold(sqlText, "information_schema") {
			return "information_schema"
		}
		return "with"
	case "INSERT":
		return "insert"
	case "UPDATE":
		return "update"
	case "DELETE":
		return "delete"
	case "REPLACE":
		return "replace"
	case "CREATE":
		switch {
		case hasPrefixFold(sqlText, "CREATE TABLE ") ||
			hasPrefixFold(sqlText, "CREATE TEMPORARY TABLE ") ||
			hasPrefixFold(sqlText, "CREATE TEMP TABLE "):
			return "create_table"
		case hasPrefixFold(sqlText, "CREATE INDEX ") ||
			hasPrefixFold(sqlText, "CREATE UNIQUE INDEX "):
			return "create_index"
		default:
			return "other"
		}
	case "ALTER":
		if hasPrefixFold(sqlText, "ALTER TABLE ") {
			return "alter_table"
		}
	case "DROP":
		switch {
		case hasPrefixFold(sqlText, "DROP TABLE "):
			return "drop_table"
		case hasPrefixFold(sqlText, "DROP INDEX "):
			return "drop_index"
		default:
			return "other"
		}
	case "PRAGMA":
		return "pragma"
	}
	switch {
	case hasPrefixFold(sqlText, "ALTER TABLE "):
		return "alter_table"
	default:
		return "other"
	}
}

func isTransactionKind(sqlText string) bool {
	return strings.EqualFold(sqlText, "BEGIN") ||
		strings.EqualFold(sqlText, "START TRANSACTION") ||
		strings.EqualFold(sqlText, "COMMIT") ||
		strings.EqualFold(sqlText, "ROLLBACK") ||
		strings.EqualFold(sqlText, "ROLLBACK AND CHAIN") ||
		hasPrefixFold(sqlText, "ROLLBACK TO SAVEPOINT ") ||
		hasPrefixFold(sqlText, "SAVEPOINT ") ||
		hasPrefixFold(sqlText, "RELEASE SAVEPOINT ")
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

func containsFold(s, substr string) bool {
	if substr == "" {
		return true
	}
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if strings.EqualFold(s[i:i+len(substr)], substr) {
			return true
		}
	}
	return false
}
