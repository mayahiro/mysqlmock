package mysqlmock

import "testing"

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
