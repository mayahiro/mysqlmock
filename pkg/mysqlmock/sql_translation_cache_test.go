package mysqlmock

import "testing"

var (
	benchmarkTranslatedSQL        string
	benchmarkTranslatedStatements []string
)

func TestTranslateSQLFastPathLeavesPlainSQLUnchanged(t *testing.T) {
	input := "SELECT id, name FROM users WHERE email = ? ORDER BY id DESC LIMIT 1"
	if got := translateSQL(input); got != input {
		t.Fatalf("translateSQL() = %q, want %q", got, input)
	}
}

func TestServerTranslateSQLCached(t *testing.T) {
	server := &Server{}

	got := server.translateSQLCached("SELECT NOW()")
	if got != "SELECT CURRENT_TIMESTAMP" {
		t.Fatalf("translateSQLCached() = %q, want CURRENT_TIMESTAMP translation", got)
	}
	got = server.translateSQLCached("SELECT NOW()")
	if got != "SELECT CURRENT_TIMESTAMP" {
		t.Fatalf("translateSQLCached() cached = %q, want CURRENT_TIMESTAMP translation", got)
	}

	server.translationMu.Lock()
	defer server.translationMu.Unlock()
	if len(server.translation.sql) != 1 {
		t.Fatalf("translation cache size = %d, want 1", len(server.translation.sql))
	}
}

func TestServerTranslateSQLCachedEvictsOneEntryAtLimit(t *testing.T) {
	server := &Server{}
	for i := range sqlTranslationCacheLimit {
		sqlText := "SELECT " + stringForCacheTestInt(i)
		if got := server.translateSQLCached(sqlText); got != sqlText {
			t.Fatalf("translateSQLCached(%q) = %q, want unchanged SQL", sqlText, got)
		}
	}

	overflowSQL := "SELECT TRUE"
	if got := server.translateSQLCached(overflowSQL); got != "SELECT 1" {
		t.Fatalf("translateSQLCached(%q) = %q, want SELECT 1", overflowSQL, got)
	}

	server.translationMu.Lock()
	defer server.translationMu.Unlock()
	if len(server.translation.sql) != sqlTranslationCacheLimit {
		t.Fatalf("translation cache size = %d, want %d", len(server.translation.sql), sqlTranslationCacheLimit)
	}
	if _, ok := server.translation.sql["SELECT 0"]; ok {
		t.Fatal("oldest translation cache entry was not evicted")
	}
	if _, ok := server.translation.sql["SELECT 1"]; !ok {
		t.Fatal("non-oldest translation cache entry was evicted")
	}
	if got, ok := server.translation.sql[overflowSQL]; !ok || got != "SELECT 1" {
		t.Fatalf("overflow translation cache entry = %q/%v, want SELECT 1/true", got, ok)
	}
}

func TestServerTranslateSQLStatementsCachedReturnsCopy(t *testing.T) {
	server := &Server{}
	input := `
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  UNIQUE KEY uniq_users_email (email)
) ENGINE=InnoDB;
`
	got := server.translateSQLStatementsCached(input)
	if len(got) != 2 {
		t.Fatalf("translateSQLStatementsCached() returned %d statements, want 2: %#v", len(got), got)
	}
	got[0] = "mutated"

	again := server.translateSQLStatementsCached(input)
	if again[0] == "mutated" {
		t.Fatalf("translateSQLStatementsCached() returned mutable cached slice: %#v", again)
	}
}

func TestServerTranslateSQLStatementsCachedEvictsOneEntryAtLimit(t *testing.T) {
	server := &Server{}
	for i := range sqlTranslationCacheLimit {
		sqlText := "SELECT " + stringForCacheTestInt(i)
		got := server.translateSQLStatementsCached(sqlText)
		if len(got) != 1 || got[0] != sqlText {
			t.Fatalf("translateSQLStatementsCached(%q) = %#v, want unchanged SQL statement", sqlText, got)
		}
	}

	overflowSQL := "SELECT FALSE"
	got := server.translateSQLStatementsCached(overflowSQL)
	if len(got) != 1 || got[0] != "SELECT 0" {
		t.Fatalf("translateSQLStatementsCached(%q) = %#v, want SELECT 0", overflowSQL, got)
	}

	server.translationMu.Lock()
	defer server.translationMu.Unlock()
	if len(server.translation.statements) != sqlTranslationCacheLimit {
		t.Fatalf("statement translation cache size = %d, want %d", len(server.translation.statements), sqlTranslationCacheLimit)
	}
	if _, ok := server.translation.statements["SELECT 0"]; ok {
		t.Fatal("oldest statement translation cache entry was not evicted")
	}
	if _, ok := server.translation.statements["SELECT 1"]; !ok {
		t.Fatal("non-oldest statement translation cache entry was evicted")
	}
	if cached, ok := server.translation.statements[overflowSQL]; !ok || len(cached) != 1 || cached[0] != "SELECT 0" {
		t.Fatalf("overflow statement translation cache entry = %#v/%v, want [SELECT 0]/true", cached, ok)
	}
}

func BenchmarkServerTranslateSQLCache(b *testing.B) {
	b.Run("sql_hit", func(b *testing.B) {
		server := &Server{}
		sqlText := "SELECT NOW()"
		server.translateSQLCached(sqlText)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkTranslatedSQL = server.translateSQLCached(sqlText)
		}
	})

	b.Run("sql_miss_without_eviction", func(b *testing.B) {
		const batchSize = 1024
		server := &Server{}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i > 0 && i%batchSize == 0 {
				b.StopTimer()
				server = &Server{}
				b.StartTimer()
			}
			benchmarkTranslatedSQL = server.translateSQLCached("SELECT TRUE /* miss " + stringForCacheTestInt(i%batchSize) + " */")
		}
	})

	b.Run("sql_eviction", func(b *testing.B) {
		server := &Server{}
		for i := range sqlTranslationCacheLimit {
			server.translateSQLCached("SELECT " + stringForCacheTestInt(i))
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkTranslatedSQL = server.translateSQLCached("SELECT TRUE /* evict " + stringForCacheTestInt(i) + " */")
		}
	})

	b.Run("sql_parallel_hit", func(b *testing.B) {
		server := &Server{}
		sqlText := "SELECT NOW()"
		server.translateSQLCached(sqlText)

		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = server.translateSQLCached(sqlText)
			}
		})
	})

	b.Run("statements_hit", func(b *testing.B) {
		server := &Server{}
		sqlText := benchmarkCreateTableSQL()
		server.translateSQLStatementsCached(sqlText)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkTranslatedStatements = server.translateSQLStatementsCached(sqlText)
		}
	})

	b.Run("statements_eviction", func(b *testing.B) {
		server := &Server{}
		for i := range sqlTranslationCacheLimit {
			server.translateSQLStatementsCached("SELECT " + stringForCacheTestInt(i))
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkTranslatedStatements = server.translateSQLStatementsCached("SELECT FALSE /* evict " + stringForCacheTestInt(i) + " */")
		}
	})
}

func BenchmarkServerTranslateSQLCachedEviction(b *testing.B) {
	server := &Server{}
	for i := range sqlTranslationCacheLimit {
		server.translateSQLCached("SELECT " + stringForCacheTestInt(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.translateSQLCached("SELECT TRUE /* " + stringForCacheTestInt(i) + " */")
	}
}

func benchmarkCreateTableSQL() string {
	return `
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  name VARCHAR(255),
  UNIQUE KEY uniq_users_email (email),
  KEY idx_users_name (name)
) ENGINE=InnoDB;
`
}

func stringForCacheTestInt(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [20]byte
	pos := len(digits)
	for n > 0 {
		pos--
		digits[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(digits[pos:])
}
