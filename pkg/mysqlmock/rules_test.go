package mysqlmock_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"

	_ "github.com/go-sql-driver/mysql"
)

func TestRulesWithGoSQLDriverMySQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := testConfig()
	cfg.Rules = []mysqlmock.RuleConfig{
		{
			Name: "exact result set",
			Request: mysqlmock.RuleRequestConfig{
				Match: "exact",
				SQL:   "SELECT 101",
			},
			Response: mysqlmock.RuleResponseConfig{
				Type:    "result_set",
				Columns: []mysqlmock.RuleColumnConfig{{Name: "value", Type: "BIGINT"}},
				Rows:    []any{[]any{int64(1001)}},
			},
		},
		{
			Name: "normalized result set",
			Request: mysqlmock.RuleRequestConfig{
				Match: "normalized",
				SQL:   "select 102",
			},
			Response: mysqlmock.RuleResponseConfig{
				Type:    "result_set",
				Columns: []mysqlmock.RuleColumnConfig{{Name: "value", Type: "BIGINT"}},
				Rows:    []any{[]any{int64(1002)}},
			},
		},
		{
			Name: "regex result set",
			Request: mysqlmock.RuleRequestConfig{
				Match: "regex",
				SQL:   `^SELECT 10[3]$`,
			},
			Response: mysqlmock.RuleResponseConfig{
				Type:    "result_set",
				Columns: []mysqlmock.RuleColumnConfig{{Name: "value", Type: "BIGINT"}},
				Rows:    []any{[]any{int64(1003)}},
			},
		},
		{
			Name: "normalized ok",
			Request: mysqlmock.RuleRequestConfig{
				Match: "normalized",
				SQL:   "set @rule_ok = 1",
			},
			Response: mysqlmock.RuleResponseConfig{
				Type:         "ok",
				AffectedRows: 2,
			},
		},
		{
			Name: "contains once error",
			Request: mysqlmock.RuleRequestConfig{
				Match: "contains",
				SQL:   "once_rule",
			},
			Response: mysqlmock.RuleResponseConfig{
				Type:     "error",
				Code:     1205,
				SQLState: "HY000",
				Message:  "Lock wait timeout exceeded",
				Once:     true,
			},
		},
		{
			Name: "any fallback",
			Request: mysqlmock.RuleRequestConfig{
				Match: "any",
			},
			Response: mysqlmock.RuleResponseConfig{
				Type:    "result_set",
				Columns: []mysqlmock.RuleColumnConfig{{Name: "value", Type: "BIGINT"}},
				Rows:    []any{[]any{int64(1999)}},
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

	assertRuleValue(t, ctx, db, "SELECT 101", 1001)
	assertRuleValue(t, ctx, db, "  SELECT\n102 ;", 1002)
	assertRuleValue(t, ctx, db, "SELECT 103", 1003)
	assertRuleOK(t, ctx, db, " SET   @rule_ok = 1;", 2)

	if err := db.QueryRowContext(ctx, "SELECT 'once_rule'").Scan(new(string)); err == nil {
		t.Fatal("expected once rule error")
	} else if !strings.Contains(err.Error(), "Lock wait timeout exceeded") {
		t.Fatalf("unexpected once rule error: %v", err)
	}
	assertRuleValue(t, ctx, db, "SELECT 'once_rule'", 1999)
	assertRuleValue(t, ctx, db, "SELECT 105", 1999)
}

func TestRuleDisconnectWithGoSQLDriverMySQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := testConfig()
	cfg.Rules = []mysqlmock.RuleConfig{
		{
			Name: "disconnect",
			Request: mysqlmock.RuleRequestConfig{
				Match: "exact",
				SQL:   "SELECT disconnect_rule",
			},
			Response: mysqlmock.RuleResponseConfig{
				Type: "disconnect",
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

	if err := db.QueryRowContext(ctx, "SELECT disconnect_rule").Scan(new(string)); err == nil {
		t.Fatal("expected disconnect rule error")
	}
}

func TestRulesLoadObjectRowsFromYAML(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	path := t.TempDir() + "/mysqlmock.yaml"
	content := []byte(`
version: 1
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
rules:
  - name: object row
    request:
      match: exact
      sql: "SELECT rule_user"
    response:
      type: result_set
      columns:
        - name: id
          type: BIGINT
        - name: name
          type: VARCHAR
      row_format: object
      rows:
        - id: 7
          name: "Rule User"
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	server := mysqlmock.Start(t, mysqlmock.ConfigFile(path))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var id int64
	var name string
	if err := db.QueryRowContext(ctx, "SELECT rule_user").Scan(&id, &name); err != nil {
		t.Fatalf("query object row rule: %v", err)
	}
	if id != 7 || name != "Rule User" {
		t.Fatalf("unexpected object row: id=%d name=%q", id, name)
	}
}

func TestRuleValidation(t *testing.T) {
	cfg := mysqlmock.DefaultConfig()
	cfg.Rules = []mysqlmock.RuleConfig{
		{
			Name: "bad regex",
			Request: mysqlmock.RuleRequestConfig{
				Match: "regex",
				SQL:   "[",
			},
			Response: mysqlmock.RuleResponseConfig{Type: "ok"},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid regex error")
	}

	cfg = mysqlmock.DefaultConfig()
	cfg.Rules = []mysqlmock.RuleConfig{
		{
			Name: "bad row",
			Request: mysqlmock.RuleRequestConfig{
				Match: "exact",
				SQL:   "SELECT 1",
			},
			Response: mysqlmock.RuleResponseConfig{
				Type:    "result_set",
				Columns: []mysqlmock.RuleColumnConfig{{Name: "one"}},
				Rows:    []any{[]any{1, 2}},
			},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid row error")
	}
}

func assertRuleValue(t *testing.T, ctx context.Context, db *sql.DB, query string, want int64) {
	t.Helper()

	var got int64
	if err := db.QueryRowContext(ctx, query).Scan(&got); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s returned no rows", query)
		}
		t.Fatalf("%s: %v", query, err)
	}
	if got != want {
		t.Fatalf("%s returned %d, want %d", query, got, want)
	}
}

func assertRuleOK(t *testing.T, ctx context.Context, db *sql.DB, query string, wantAffected int64) {
	t.Helper()

	result, err := db.ExecContext(ctx, query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	got, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("%s rows affected: %v", query, err)
	}
	if got != wantAffected {
		t.Fatalf("%s affected %d rows, want %d", query, got, wantAffected)
	}
}
