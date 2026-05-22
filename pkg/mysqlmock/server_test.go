package mysqlmock_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"

	_ "github.com/go-sql-driver/mysql"
)

func TestServerWithGoSQLDriverMySQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatalf("select version: %v", err)
	}
	if version != "8.0.36-mock" {
		t.Fatalf("unexpected version: %s", version)
	}

	if _, err := db.ExecContext(ctx, "SET NAMES latin1 COLLATE latin1_swedish_ci"); err != nil {
		t.Fatalf("set names: %v", err)
	}
	for query, want := range map[string]string{
		"SELECT @@character_set_client":     "latin1",
		"SELECT @@character_set_connection": "latin1",
		"SELECT @@character_set_results":    "latin1",
		"SELECT @@collation_connection":     "latin1_swedish_ci",
	} {
		var got string
		if err := db.QueryRowContext(ctx, query).Scan(&got); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if got != want {
			t.Fatalf("%s returned %q, want %q", query, got, want)
		}
	}
	assertShowVariable(t, ctx, db, "character_set_results", "latin1")

	if _, err := db.ExecContext(ctx, "SET autocommit = 0"); err != nil {
		t.Fatalf("set autocommit off: %v", err)
	}
	var autocommit string
	if err := db.QueryRowContext(ctx, "SELECT @@autocommit").Scan(&autocommit); err != nil {
		t.Fatalf("select autocommit: %v", err)
	}
	if autocommit != "0" {
		t.Fatalf("unexpected autocommit: %s", autocommit)
	}
	if _, err := db.ExecContext(ctx, "SET autocommit = 1"); err != nil {
		t.Fatalf("set autocommit on: %v", err)
	}

	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE id = ?", 1).Scan(&name); err != nil {
		t.Fatalf("select seed row: %v", err)
	}
	if name != "Alice" {
		t.Fatalf("unexpected seed name: %s", name)
	}

	insert, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Carol", "carol@example.com")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	insertID, err := insert.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if insertID == 0 {
		t.Fatal("expected non-zero last insert id")
	}

	update, err := db.ExecContext(ctx, "UPDATE users SET name = ? WHERE email = ?", "Caroline", "carol@example.com")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	updated, err := update.RowsAffected()
	if err != nil {
		t.Fatalf("update rows affected: %v", err)
	}
	if updated != 1 {
		t.Fatalf("unexpected update rows affected: %d", updated)
	}

	var updatedName string
	if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE email = ?", "carol@example.com").Scan(&updatedName); err != nil {
		t.Fatalf("select inserted row: %v", err)
	}
	if updatedName != "Caroline" {
		t.Fatalf("unexpected updated name: %s", updatedName)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin rollback tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Rollback", "rollback@example.com"); err != nil {
		t.Fatalf("insert in rollback tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE email = ?", "rollback@example.com").Scan(&name); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected rollback row to disappear, got: %v", err)
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin commit tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Commit", "commit@example.com"); err != nil {
		t.Fatalf("insert in commit tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE email = ?", "commit@example.com").Scan(&name); err != nil {
		t.Fatalf("select committed row: %v", err)
	}
	if name != "Commit" {
		t.Fatalf("unexpected committed name: %s", name)
	}

	if _, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Duplicate", "alice@example.com"); err == nil {
		t.Fatal("expected duplicate email error")
	} else if !strings.Contains(err.Error(), "Duplicate entry") {
		t.Fatalf("unexpected duplicate error: %v", err)
	}

	stmt, err := db.PrepareContext(ctx, "SELECT name FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("prepare select: %v", err)
	}
	defer stmt.Close()
	if err := stmt.QueryRowContext(ctx, 2).Scan(&name); err != nil {
		t.Fatalf("prepared select: %v", err)
	}
	if name != "Bob" {
		t.Fatalf("unexpected prepared select name: %s", name)
	}

	insertStmt, err := db.PrepareContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("prepare insert: %v", err)
	}
	defer insertStmt.Close()
	preparedInsert, err := insertStmt.ExecContext(ctx, "Prepared", "prepared@example.com")
	if err != nil {
		t.Fatalf("prepared insert: %v", err)
	}
	preparedInsertID, err := preparedInsert.LastInsertId()
	if err != nil {
		t.Fatalf("prepared last insert id: %v", err)
	}
	if preparedInsertID == 0 {
		t.Fatal("expected prepared insert id")
	}
}

func TestServerWithGoSQLDriverMySQLPreparedDirectQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	dsn := fmt.Sprintf("user:password@tcp(%s)/mysqlmock?charset=utf8mb4&parseTime=true", server.Addr())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE id = ?", 1).Scan(&name); err != nil {
		t.Fatalf("prepared direct query select: %v", err)
	}
	if name != "Alice" {
		t.Fatalf("unexpected prepared direct query name: %s", name)
	}

	result, err := db.ExecContext(ctx, "UPDATE users SET name = ? WHERE id = ?", "Alicia", 1)
	if err != nil {
		t.Fatalf("prepared direct query update: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("prepared direct query rows affected: %v", err)
	}
	if affected != 1 {
		t.Fatalf("unexpected prepared direct query rows affected: %d", affected)
	}

	var gotInt int64
	var gotText string
	var gotBytes []byte
	var gotNull sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT ?, ?, ?, ?", int64(42), "hello", []byte("bytes"), nil).Scan(&gotInt, &gotText, &gotBytes, &gotNull); err != nil {
		t.Fatalf("prepared direct query scalar round trip: %v", err)
	}
	if gotInt != 42 || gotText != "hello" || string(gotBytes) != "bytes" || gotNull.Valid {
		t.Fatalf("unexpected scalar round trip: int=%d text=%q bytes=%q null=%v", gotInt, gotText, gotBytes, gotNull.Valid)
	}
}

func TestCheckConfigFile(t *testing.T) {
	path := t.TempDir() + "/mysqlmock.yaml"
	content := []byte(`
version: 1
server:
  mysql_version: "8.0.36-mock"
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
schema:
  - |
    CREATE TABLE users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE
    );
seed:
  users:
    - id: 1
      name: "Alice"
      email: "alice@example.com"
fallback:
  type: sqlite
  unsupported:
    type: error
    code: 1105
    sql_state: "HY000"
    message: "Unsupported query"
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := mysqlmock.CheckConfigFile(context.Background(), path); err != nil {
		t.Fatalf("check config: %v", err)
	}
}

func TestServerResetReappliesSchemaSeedAndClearsRuntimeState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := testConfig()
	cfg.Rules = []mysqlmock.RuleConfig{
		{
			Name: "once reset rule",
			Request: mysqlmock.RuleRequestConfig{
				Match: "contains",
				SQL:   "reset_once",
			},
			Response: mysqlmock.RuleResponseConfig{
				Type:     "error",
				Code:     1205,
				SQLState: "HY000",
				Message:  "Lock wait timeout exceeded",
				Once:     true,
			},
		},
	}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Reset", "reset@example.com"); err != nil {
		t.Fatalf("insert before reset: %v", err)
	}
	assertUserCount(t, ctx, db, 3)

	if err := db.QueryRowContext(ctx, "SELECT 'reset_once'").Scan(new(string)); err == nil {
		t.Fatal("expected once rule error before reset")
	}
	if err := db.QueryRowContext(ctx, "SELECT 'reset_once'").Scan(new(string)); err != nil {
		t.Fatalf("expected once rule to be exhausted before reset: %v", err)
	}

	if _, err := db.ExecContext(ctx, "CREATE USER reset_unsupported"); err == nil {
		t.Fatal("expected unsupported query before reset")
	}
	if len(server.Unsupported()) != 1 {
		t.Fatalf("unsupported count before reset = %d, want 1", len(server.Unsupported()))
	}

	if err := server.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	assertUserCount(t, ctx, db, 2)
	if len(server.Unsupported()) != 0 {
		t.Fatalf("unsupported count after reset = %d, want 0", len(server.Unsupported()))
	}
	if err := db.QueryRowContext(ctx, "SELECT 'reset_once'").Scan(new(string)); err == nil {
		t.Fatal("expected once rule error after reset")
	}
}

func TestServerResetBeforeStart(t *testing.T) {
	server, err := mysqlmock.New(mysqlmock.WithConfig(testConfig()))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Reset(context.Background()); err == nil {
		t.Fatal("expected reset before start error")
	}
}

func TestQueryLoggingTextRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var logs bytes.Buffer
	server := mysqlmock.Start(t,
		mysqlmock.WithConfig(testConfig()),
		mysqlmock.LogWriter(&logs),
		mysqlmock.LogFormat("text"),
	)
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatalf("select version: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Logged", "logged@example.com"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got := logs.String()
	for _, want := range []string{
		`command=COM_QUERY route=compat`,
		`sql="SELECT VERSION()"`,
		`command=COM_QUERY route=sqlite`,
		`INSERT INTO users`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("logs %q do not contain %q", got, want)
		}
	}
}

func TestQueryLoggingJSONRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var logs bytes.Buffer
	server := mysqlmock.Start(t,
		mysqlmock.WithConfig(testConfig()),
		mysqlmock.LogWriter(&logs),
		mysqlmock.LogFormat("json"),
	)
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatalf("select version: %v", err)
	}

	found := false
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		if event["event"] == "query" &&
			event["command"] == "COM_QUERY" &&
			event["route"] == "compat" &&
			event["database"] == "mysqlmock" &&
			event["sql"] == "SELECT VERSION()" {
			found = true
		}
	}
	if !found {
		t.Fatalf("compat query log not found in %q", logs.String())
	}
}

func TestLogFormatValidation(t *testing.T) {
	if _, err := mysqlmock.New(mysqlmock.LogFormat("yaml")); err == nil {
		t.Fatal("expected unsupported log format error")
	}
}

func TestFallbackUnsupportedConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := testConfig()
	cfg.Fallback.Unsupported = mysqlmock.UnsupportedConfig{
		Type:     "error",
		Code:     1644,
		SQLState: "45000",
		Message:  "Rejected by mysqlmock",
	}

	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(ctx, "CREATE USER unsupported_user")
	if err == nil {
		t.Fatal("expected unsupported query error")
	}
	if !strings.Contains(err.Error(), "Error 1644 (45000): Rejected by mysqlmock: CREATE USER unsupported_user") {
		t.Fatalf("unexpected unsupported query error: %v", err)
	}

	unsupported := server.Unsupported()
	if len(unsupported) != 1 {
		t.Fatalf("unsupported query count = %d, want 1", len(unsupported))
	}
	if unsupported[0].SQL != "CREATE USER unsupported_user" {
		t.Fatalf("unsupported SQL = %q", unsupported[0].SQL)
	}
	if unsupported[0].NormalizedSQL != "CREATE USER unsupported_user" {
		t.Fatalf("unsupported normalized SQL = %q", unsupported[0].NormalizedSQL)
	}
	if unsupported[0].ConnectionID == 0 {
		t.Fatal("unsupported connection id was not recorded")
	}
	if unsupported[0].Command != "COM_QUERY" {
		t.Fatalf("unsupported command = %q", unsupported[0].Command)
	}
	if unsupported[0].CurrentDB != "mysqlmock" {
		t.Fatalf("unsupported current DB = %q", unsupported[0].CurrentDB)
	}
	if unsupported[0].RouteStage != "unsupported" {
		t.Fatalf("unsupported route stage = %q", unsupported[0].RouteStage)
	}
	for _, want := range []string{
		"code: 1644",
		`sql_state: "45000"`,
		`message: "Rejected by mysqlmock"`,
	} {
		if !strings.Contains(unsupported[0].Suggestion, want) {
			t.Fatalf("unsupported suggestion %q does not contain %q", unsupported[0].Suggestion, want)
		}
	}
}

func TestUnsupportedQueryRecordsCompatRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if err := db.QueryRowContext(ctx, " SELECT   @@session.unknown_variable ;").Scan(new(string)); err == nil {
		t.Fatal("expected unsupported compat variable error")
	}

	unsupported := server.Unsupported()
	if len(unsupported) != 1 {
		t.Fatalf("unsupported query count = %d, want 1", len(unsupported))
	}
	if !strings.Contains(unsupported[0].SQL, "@@session.unknown_variable") {
		t.Fatalf("unsupported SQL = %q", unsupported[0].SQL)
	}
	if unsupported[0].NormalizedSQL != "SELECT @@session.unknown_variable" {
		t.Fatalf("unsupported normalized SQL = %q", unsupported[0].NormalizedSQL)
	}
	if unsupported[0].RouteStage != "compat" {
		t.Fatalf("unsupported route stage = %q", unsupported[0].RouteStage)
	}
}

func TestUnsupportedQueryRecordsSQLiteRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES ()"); err == nil {
		t.Fatal("expected sqlite syntax error")
	}

	unsupported := server.Unsupported()
	if len(unsupported) != 1 {
		t.Fatalf("unsupported query count = %d, want 1", len(unsupported))
	}
	if unsupported[0].RouteStage != "sqlite" {
		t.Fatalf("unsupported route stage = %q", unsupported[0].RouteStage)
	}
}

func TestFallbackUnsupportedValidation(t *testing.T) {
	cfg := mysqlmock.DefaultConfig()
	cfg.Fallback = mysqlmock.FallbackConfig{}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate without fallback: %v", err)
	}

	cfg = mysqlmock.DefaultConfig()
	cfg.Fallback.Unsupported.Type = "ok"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid fallback.unsupported.type error")
	}

	cfg = mysqlmock.DefaultConfig()
	cfg.Fallback.Unsupported.SQLState = "HY00"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid fallback.unsupported.sql_state error")
	}
}

func assertShowVariable(t *testing.T, ctx context.Context, db *sql.DB, name, want string) {
	t.Helper()

	rows, err := db.QueryContext(ctx, "SHOW VARIABLES")
	if err != nil {
		t.Fatalf("show variables: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var gotName string
		var gotValue string
		if err := rows.Scan(&gotName, &gotValue); err != nil {
			t.Fatalf("scan show variables: %v", err)
		}
		if gotName != name {
			continue
		}
		if gotValue != want {
			t.Fatalf("SHOW VARIABLES %s = %q, want %q", name, gotValue, want)
		}
		return
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("show variables rows: %v", err)
	}
	t.Fatalf("SHOW VARIABLES did not include %s", name)
}

func assertUserCount(t *testing.T, ctx context.Context, db *sql.DB, want int) {
	t.Helper()

	var got int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&got); err != nil {
		t.Fatalf("select user count: %v", err)
	}
	if got != want {
		t.Fatalf("user count = %d, want %d", got, want)
	}
}

func testConfig() mysqlmock.Config {
	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE
);`}
	cfg.Seed = map[string][]map[string]any{
		"users": {
			{
				"id":    1,
				"name":  "Alice",
				"email": "alice@example.com",
			},
			{
				"id":    2,
				"name":  "Bob",
				"email": "bob@example.com",
			},
		},
	}
	return cfg
}
