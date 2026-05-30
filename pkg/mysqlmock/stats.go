package mysqlmock

import (
	"strings"
	"sync"
)

// Stats is a SQL-body-free snapshot of server execution counters.
type Stats struct {
	Queries       QueryStats    `json:"queries"`
	Metadata      MetadataStats `json:"metadata"`
	Resets        ResetStats    `json:"resets"`
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
	InformationSchemaQueries         uint64 `json:"information_schema_queries"`
	TargetedInformationSchemaQueries uint64 `json:"targeted_information_schema_queries"`
	BroadInformationSchemaQueries    uint64 `json:"broad_information_schema_queries"`
	ShowFullFieldsQueries            uint64 `json:"show_full_fields_queries"`
	ShowCreateTableQueries           uint64 `json:"show_create_table_queries"`
	ShowKeysQueries                  uint64 `json:"show_keys_queries"`
	TargetTableRefreshes             uint64 `json:"target_table_refreshes"`
	TargetTableCacheHits             uint64 `json:"target_table_cache_hits"`
	TargetTableMisses                uint64 `json:"target_table_misses"`
	FullRefreshes                    uint64 `json:"full_refreshes"`
	FullRefreshCacheHits             uint64 `json:"full_refresh_cache_hits"`
	TablesLoaded                     uint64 `json:"tables_loaded"`
}

// ResetStats summarizes successful Server.Reset calls.
type ResetStats struct {
	Total    uint64 `json:"total"`
	DataOnly uint64 `json:"data_only"`
	Full     uint64 `json:"full"`
}

type statsStore struct {
	mu            sync.Mutex
	queries       QueryStats
	metadata      MetadataStats
	resets        ResetStats
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
		Metadata:      s.metadata,
		Resets:        s.resets,
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

func (s *statsStore) recordInformationSchemaQuery(targeted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata.InformationSchemaQueries++
	if targeted {
		s.metadata.TargetedInformationSchemaQueries++
	} else {
		s.metadata.BroadInformationSchemaQueries++
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
