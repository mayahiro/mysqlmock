package mysqlmock_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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

func TestAutoIncrementRollbackDoesNotReuseValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin rollback tx: %v", err)
	}
	rolledBack, err := tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Rollback", "rollback@example.com")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert rollback row: %v", err)
	}
	rolledBackID, err := rolledBack.LastInsertId()
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("rollback row last insert id: %v", err)
	}
	if rolledBackID != 3 {
		_ = tx.Rollback()
		t.Fatalf("rollback row id = %d, want 3", rolledBackID)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	inserted, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "After Rollback", "after-rollback@example.com")
	if err != nil {
		t.Fatalf("insert after rollback: %v", err)
	}
	insertedID, err := inserted.LastInsertId()
	if err != nil {
		t.Fatalf("after rollback last insert id: %v", err)
	}
	if insertedID != 4 {
		t.Fatalf("after rollback id = %d, want 4", insertedID)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE email = ?", "rollback@example.com").Scan(&count); err != nil {
		t.Fatalf("count rollback row: %v", err)
	}
	if count != 0 {
		t.Fatalf("rollback row count = %d, want 0", count)
	}
}

func TestAutoIncrementSavepointRollbackDoesNotReuseValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	kept, err := tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Keep", "keep-savepoint@example.com")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert kept row: %v", err)
	}
	keptID, err := kept.LastInsertId()
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("kept row last insert id: %v", err)
	}
	if keptID != 3 {
		_ = tx.Rollback()
		t.Fatalf("kept row id = %d, want 3", keptID)
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT sp1"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("savepoint: %v", err)
	}
	discarded, err := tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Discard", "discard-savepoint@example.com")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert discarded row: %v", err)
	}
	discardedID, err := discarded.LastInsertId()
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("discarded row last insert id: %v", err)
	}
	if discardedID != 4 {
		_ = tx.Rollback()
		t.Fatalf("discarded row id = %d, want 4", discardedID)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp1"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("rollback to savepoint: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT sp1"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("release savepoint: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	inserted, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "After Savepoint", "after-savepoint@example.com")
	if err != nil {
		t.Fatalf("insert after savepoint rollback: %v", err)
	}
	insertedID, err := inserted.LastInsertId()
	if err != nil {
		t.Fatalf("after savepoint last insert id: %v", err)
	}
	if insertedID != 5 {
		t.Fatalf("after savepoint id = %d, want 5", insertedID)
	}
}

func TestSelectVersionAlias(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	for _, tt := range []struct {
		query      string
		columnName string
	}{
		{query: "SELECT VERSION() AS x", columnName: "x"},
		{query: "SELECT VERSION() version_alias", columnName: "version_alias"},
		{query: "SELECT VERSION AS `mysql_version`", columnName: "mysql_version"},
	} {
		rows, err := db.QueryContext(ctx, tt.query)
		if err != nil {
			t.Fatalf("%s: %v", tt.query, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatalf("%s columns: %v", tt.query, err)
		}
		if len(columns) != 1 || columns[0] != tt.columnName {
			_ = rows.Close()
			t.Fatalf("%s columns = %#v, want %q", tt.query, columns, tt.columnName)
		}
		if !rows.Next() {
			_ = rows.Close()
			t.Fatalf("%s returned no rows", tt.query)
		}
		var version string
		if err := rows.Scan(&version); err != nil {
			_ = rows.Close()
			t.Fatalf("%s scan: %v", tt.query, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("%s close rows: %v", tt.query, err)
		}
		if version != "8.0.36-mock" {
			t.Fatalf("%s returned version %q, want 8.0.36-mock", tt.query, version)
		}
	}
}

func TestMySQLStringLiteralLikeAndQualifiedUpdateCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE search_items (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  name TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 0,
  payload TEXT NULL
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, `INSERT INTO search_items (name, payload) VALUES ('Bob\'s 100% ready', '{\"Company\":\"Acme\"}')`); err != nil {
		t.Fatalf("insert MySQL escaped string literal: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO search_items (name, payload) VALUES ('100X ready', NULL)`); err != nil {
		t.Fatalf("insert comparison row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO search_items (name, payload) VALUES ('under_score', NULL)`); err != nil {
		t.Fatalf("insert underscore row: %v", err)
	}

	var payload string
	if err := db.QueryRowContext(ctx, "SELECT payload FROM search_items WHERE id = 1").Scan(&payload); err != nil {
		t.Fatalf("select JSON payload: %v", err)
	}
	if payload != `{"Company":"Acme"}` {
		t.Fatalf("payload = %q, want unescaped JSON", payload)
	}

	var likeCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM search_items WHERE name LIKE 'Bob\'s 100\%%'`).Scan(&likeCount); err != nil {
		t.Fatalf("LIKE with default MySQL backslash escape: %v", err)
	}
	if likeCount != 1 {
		t.Fatalf("LIKE percent count = %d, want 1", likeCount)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM search_items WHERE name LIKE 'under\_score'`).Scan(&likeCount); err != nil {
		t.Fatalf("LIKE underscore default MySQL backslash escape: %v", err)
	}
	if likeCount != 1 {
		t.Fatalf("LIKE underscore count = %d, want 1", likeCount)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM search_items WHERE name LIKE 'Bob!_s 100!%%' ESCAPE '!'`).Scan(&likeCount); err != nil {
		t.Fatalf("LIKE with explicit escape: %v", err)
	}
	if likeCount != 0 {
		t.Fatalf("explicit ESCAPE LIKE count = %d, want 0", likeCount)
	}

	if _, err := db.ExecContext(ctx, `UPDATE search_items SET search_items.name = 'Updated' WHERE search_items.id = 1`); err != nil {
		t.Fatalf("table-qualified UPDATE SET target: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE search_items SET search_items.enabled = 1 WHERE search_items.id = 2`); err != nil {
		t.Fatalf("table-qualified UPDATE integer SET target: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE search_items SET search_items.enabled = TRUE WHERE search_items.id = 3`); err != nil {
		t.Fatalf("table-qualified UPDATE TRUE SET target: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE search_items SET search_items.enabled = FALSE WHERE search_items.id = 1`); err != nil {
		t.Fatalf("table-qualified UPDATE FALSE SET target: %v", err)
	}
	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM search_items WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("select updated row: %v", err)
	}
	if name != "Updated" {
		t.Fatalf("updated name = %q, want Updated", name)
	}
	var enabled int
	if err := db.QueryRowContext(ctx, "SELECT enabled FROM search_items WHERE id = 2").Scan(&enabled); err != nil {
		t.Fatalf("select integer-updated row: %v", err)
	}
	if enabled != 1 {
		t.Fatalf("integer-updated enabled = %d, want 1", enabled)
	}
	if err := db.QueryRowContext(ctx, "SELECT enabled FROM search_items WHERE id = 3").Scan(&enabled); err != nil {
		t.Fatalf("select TRUE-updated row: %v", err)
	}
	if enabled != 1 {
		t.Fatalf("TRUE-updated enabled = %d, want 1", enabled)
	}
	if err := db.QueryRowContext(ctx, "SELECT enabled FROM search_items WHERE id = 1").Scan(&enabled); err != nil {
		t.Fatalf("select FALSE-updated row: %v", err)
	}
	if enabled != 0 {
		t.Fatalf("FALSE-updated enabled = %d, want 0", enabled)
	}
}

func TestForeignKeyConstraintViolationMapsToMySQLError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{
		`CREATE TABLE parents (id INTEGER PRIMARY KEY);`,
		`CREATE TABLE children (
			id INTEGER PRIMARY KEY,
			parent_id INTEGER NOT NULL,
			FOREIGN KEY(parent_id) REFERENCES parents(id)
		);`,
	}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(ctx, "INSERT INTO children (id, parent_id) VALUES (?, ?)", 1, 999)
	if err == nil {
		t.Fatal("expected foreign key violation")
	}
	if !strings.Contains(err.Error(), "Error 1452") || !strings.Contains(err.Error(), "Cannot add or update child row") {
		t.Fatalf("unexpected foreign key error: %v", err)
	}

	var refTable string
	var refColumn string
	if err := db.QueryRowContext(ctx, `
SELECT REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
FROM information_schema.key_column_usage
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'children'
  AND COLUMN_NAME = 'parent_id'
`).Scan(&refTable, &refColumn); err != nil {
		t.Fatalf("query information_schema.key_column_usage foreign key: %v", err)
	}
	if refTable != "parents" || refColumn != "id" {
		t.Fatalf("foreign key reference = %s.%s, want parents.id", refTable, refColumn)
	}

	var constraintType string
	if err := db.QueryRowContext(ctx, `
SELECT CONSTRAINT_TYPE
FROM information_schema.table_constraints
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'children'
  AND CONSTRAINT_NAME LIKE 'fk_children_%'
`).Scan(&constraintType); err != nil {
		t.Fatalf("query information_schema.table_constraints foreign key: %v", err)
	}
	if constraintType != "FOREIGN KEY" {
		t.Fatalf("foreign key constraint type = %q, want FOREIGN KEY", constraintType)
	}

	var updateRule string
	var deleteRule string
	var referencedTable string
	if err := db.QueryRowContext(ctx, `
SELECT UPDATE_RULE, DELETE_RULE, REFERENCED_TABLE_NAME
FROM information_schema.referential_constraints
WHERE CONSTRAINT_SCHEMA = DATABASE()
  AND TABLE_NAME = 'children'
  AND CONSTRAINT_NAME LIKE 'fk_children_%'
`).Scan(&updateRule, &deleteRule, &referencedTable); err != nil {
		t.Fatalf("query information_schema.referential_constraints: %v", err)
	}
	if updateRule != "NO ACTION" || deleteRule != "NO ACTION" || referencedTable != "parents" {
		t.Fatalf("referential constraint = update:%q delete:%q ref:%q, want NO ACTION/NO ACTION/parents", updateRule, deleteRule, referencedTable)
	}
}

func TestNotNullConstraintViolationMapsToMySQLError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(ctx, "INSERT INTO users (email) VALUES (?)", "missing-name@example.com")
	if err == nil {
		t.Fatal("expected not null violation")
	}
	if !strings.Contains(err.Error(), "Error 1048") || !strings.Contains(err.Error(), "Column cannot be null") {
		t.Fatalf("unexpected not null error: %v", err)
	}
}

func TestWriteValueValidationMapsToMySQLErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE strict_values (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  short_name VARCHAR(3) NOT NULL,
  count_value INTEGER NOT NULL,
  created_at DATETIME NOT NULL,
  checked_value INTEGER NOT NULL CHECK (checked_value > 0)
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	cases := []struct {
		name    string
		query   string
		args    []any
		wantErr string
	}{
		{
			name:    "data too long",
			query:   "INSERT INTO strict_values (short_name, count_value, created_at, checked_value) VALUES (?, ?, ?, ?)",
			args:    []any{"toolong", 1, "2026-05-28 10:11:12", 1},
			wantErr: "Error 1406 (22001): Data too long for column 'short_name' at row 1",
		},
		{
			name:    "incorrect integer",
			query:   "INSERT INTO strict_values (short_name, count_value, created_at, checked_value) VALUES (?, ?, ?, ?)",
			args:    []any{"ok", "abc", "2026-05-28 10:11:12", 1},
			wantErr: "Error 1366 (HY000): Incorrect integer value: 'abc' for column 'count_value' at row 1",
		},
		{
			name:    "incorrect datetime",
			query:   "INSERT INTO strict_values (short_name, count_value, created_at, checked_value) VALUES (?, ?, ?, ?)",
			args:    []any{"ok", 1, "not-a-date", 1},
			wantErr: "Error 1292 (22007): Incorrect datetime value: 'not-a-date' for column 'created_at' at row 1",
		},
		{
			name:    "zero datetime rejected by default",
			query:   "INSERT INTO strict_values (short_name, count_value, created_at, checked_value) VALUES (?, ?, ?, ?)",
			args:    []any{"ok", 1, "0000-00-00 00:00:00", 1},
			wantErr: "Error 1292 (22007): Incorrect datetime value: '0000-00-00 00:00:00' for column 'created_at' at row 1",
		},
		{
			name:    "zero in date rejected by default",
			query:   "INSERT INTO strict_values (short_name, count_value, created_at, checked_value) VALUES (?, ?, ?, ?)",
			args:    []any{"ok", 1, "0001-00-00 00:00:00", 1},
			wantErr: "Error 1292 (22007): Incorrect datetime value: '0001-00-00 00:00:00' for column 'created_at' at row 1",
		},
		{
			name:    "check constraint",
			query:   "INSERT INTO strict_values (short_name, count_value, created_at, checked_value) VALUES (?, ?, ?, ?)",
			args:    []any{"ok", 1, "2026-05-28 10:11:12", 0},
			wantErr: "Error 3819 (HY000): Check constraint failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, tc.query, tc.args...); err == nil {
				t.Fatal("expected MySQL-like write error")
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}

	if _, err := db.ExecContext(ctx, "INSERT INTO strict_values (short_name, count_value, created_at, checked_value) VALUES (?, ?, ?, ?)", "ok", 1, "2026-05-28 10:11:12", 1); err != nil {
		t.Fatalf("valid strict value insert: %v", err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE strict_values SET short_name = ? WHERE id = ?", "long", 1); err == nil {
		t.Fatal("expected update data too long error")
	} else if !strings.Contains(err.Error(), "Error 1406 (22001): Data too long for column 'short_name' at row 1") {
		t.Fatalf("update error = %v, want data too long", err)
	}
}

func TestAllowZeroDatesCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Compat.AllowZeroDates = true
	cfg.Schema = []string{`
CREATE TABLE zero_date_values (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  date_value DATE NOT NULL,
  datetime_value DATETIME NOT NULL
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "INSERT INTO zero_date_values (date_value, datetime_value) VALUES (?, ?)", "0000-00-00", "0000-00-00 00:00:00"); err != nil {
		t.Fatalf("insert all-zero date: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO zero_date_values (date_value, datetime_value) VALUES (?, ?)", "0001-00-00", "0001-00-00 00:00:00"); err != nil {
		t.Fatalf("insert zero-in-date: %v", err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE zero_date_values SET datetime_value = ? WHERE id = 2", "0000-00-00 00:00:00.123456"); err != nil {
		t.Fatalf("update zero datetime: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO zero_date_values (date_value, datetime_value) VALUES (?, ?)", "2026-13-01", "2026-05-28 10:11:12"); err == nil {
		t.Fatal("expected non-zero invalid date to remain rejected")
	} else if !strings.Contains(err.Error(), "Error 1292") {
		t.Fatalf("invalid date error = %v, want Error 1292", err)
	}

	rows, err := db.QueryContext(ctx, "SELECT CAST(date_value AS TEXT), CAST(datetime_value AS TEXT) FROM zero_date_values ORDER BY id")
	if err != nil {
		t.Fatalf("select zero dates: %v", err)
	}
	defer rows.Close()

	got := [][2]string{}
	for rows.Next() {
		var dateValue string
		var datetimeValue string
		if err := rows.Scan(&dateValue, &datetimeValue); err != nil {
			t.Fatalf("scan zero dates: %v", err)
		}
		got = append(got, [2]string{dateValue, datetimeValue})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read zero dates: %v", err)
	}
	want := [][2]string{
		{"0000-00-00", "0000-00-00 00:00:00"},
		{"0001-00-00", "0000-00-00 00:00:00.123456"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("zero date rows = %#v, want %#v", got, want)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func TestWriteValidationModes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	newDB := func(t *testing.T, mode string) (*mysqlmock.Server, *sql.DB) {
		t.Helper()

		cfg := mysqlmock.DefaultConfig()
		cfg.Compat.WriteValidation = mode
		cfg.Schema = []string{`
CREATE TABLE write_validation_values (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  short_name VARCHAR(3) NOT NULL,
  required_name TEXT NOT NULL
);`}
		server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
		db, err := sql.Open("mysql", server.DSN())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = db.Close()
		})
		db.SetMaxOpenConns(1)
		return server, db
	}

	t.Run("strict pre-validates MySQL-like value errors", func(t *testing.T) {
		server, db := newDB(t, "strict")
		if _, err := db.ExecContext(ctx, "INSERT INTO write_validation_values (short_name, required_name) VALUES (?, ?)", "toolong", "Strict"); err == nil {
			t.Fatal("expected strict write validation error")
		} else if !strings.Contains(err.Error(), "Error 1406") {
			t.Fatalf("strict validation error = %v, want Error 1406", err)
		}
		mysqlmock.AssertNoUnsupported(t, server)
	})

	t.Run("basic skips pre-validation but keeps SQLite error mapping", func(t *testing.T) {
		server, db := newDB(t, "basic")
		if _, err := db.ExecContext(ctx, "INSERT INTO write_validation_values (short_name, required_name) VALUES (?, ?)", "toolong", "Basic"); err != nil {
			t.Fatalf("basic write should skip length pre-validation: %v", err)
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO write_validation_values (short_name, required_name) VALUES (?, ?)", "ok", nil); err == nil {
			t.Fatal("expected SQLite NOT NULL error mapping")
		} else if !strings.Contains(err.Error(), "Error 1048") || !strings.Contains(err.Error(), "Column cannot be null") {
			t.Fatalf("basic not-null error = %v, want mapped MySQL error", err)
		}
		mysqlmock.AssertNoUnsupported(t, server)
	})

	t.Run("off skips pre-validation and SQLite error mapping", func(t *testing.T) {
		server, db := newDB(t, "off")
		if _, err := db.ExecContext(ctx, "INSERT INTO write_validation_values (short_name, required_name) VALUES (?, ?)", "ok", nil); err == nil {
			t.Fatal("expected raw SQLite NOT NULL error")
		} else if !strings.Contains(err.Error(), "Error 1105") || strings.Contains(err.Error(), "Column cannot be null") {
			t.Fatalf("off not-null error = %v, want generic SQLite error", err)
		}
		mysqlmock.AssertNoUnsupported(t, server)
	})
}

func TestSavepointRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Keep", "keep-savepoint@example.com"); err != nil {
		t.Fatalf("insert before savepoint: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT sp1"); err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Discard", "discard-savepoint@example.com"); err != nil {
		t.Fatalf("insert after savepoint: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp1"); err != nil {
		t.Fatalf("rollback to savepoint: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT sp1"); err != nil {
		t.Fatalf("release savepoint: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE email = ?", "keep-savepoint@example.com").Scan(&name); err != nil {
		t.Fatalf("select kept row: %v", err)
	}
	if name != "Keep" {
		t.Fatalf("kept row name = %q, want Keep", name)
	}
	err = db.QueryRowContext(ctx, "SELECT name FROM users WHERE email = ?", "discard-savepoint@example.com").Scan(&name)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected savepoint row to be rolled back, got name=%q err=%v", name, err)
	}
}

func TestInformationSchemaTablesAndColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var tableName string
	var tableType string
	var engine string
	if err := db.QueryRowContext(ctx, `
SELECT TABLE_NAME, TABLE_TYPE, ENGINE
FROM information_schema.tables
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users'
`).Scan(&tableName, &tableType, &engine); err != nil {
		t.Fatalf("query information_schema.tables: %v", err)
	}
	if tableName != "users" || tableType != "BASE TABLE" || engine != "SQLite" {
		t.Fatalf("unexpected information_schema.tables row: table=%q type=%q engine=%q", tableName, tableType, engine)
	}

	rows, err := db.QueryContext(ctx, `
SELECT COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, COLUMN_KEY
FROM information_schema.columns
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users'
ORDER BY ORDINAL_POSITION
`)
	if err != nil {
		t.Fatalf("query information_schema.columns: %v", err)
	}
	defer rows.Close()

	type columnInfo struct {
		name            string
		ordinalPosition int
		isNullable      string
		dataType        string
		columnKey       string
	}
	var got []columnInfo
	for rows.Next() {
		var col columnInfo
		if err := rows.Scan(&col.name, &col.ordinalPosition, &col.isNullable, &col.dataType, &col.columnKey); err != nil {
			t.Fatalf("scan information_schema.columns: %v", err)
		}
		got = append(got, col)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read information_schema.columns: %v", err)
	}

	want := []columnInfo{
		{name: "id", ordinalPosition: 1, isNullable: "NO", dataType: "integer", columnKey: "PRI"},
		{name: "name", ordinalPosition: 2, isNullable: "NO", dataType: "text", columnKey: ""},
		{name: "email", ordinalPosition: 3, isNullable: "NO", dataType: "text", columnKey: ""},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("information_schema.columns = %#v, want %#v", got, want)
	}

	var schemaName string
	var charset string
	if err := db.QueryRowContext(ctx, `
SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME
FROM information_schema.schemata
WHERE SCHEMA_NAME = DATABASE()
`).Scan(&schemaName, &charset); err != nil {
		t.Fatalf("query information_schema.schemata: %v", err)
	}
	if schemaName != "mysqlmock" || charset != "utf8mb4" {
		t.Fatalf("unexpected information_schema.schemata row: schema=%q charset=%q", schemaName, charset)
	}

	var primaryOrdinal int
	if err := db.QueryRowContext(ctx, `
SELECT ORDINAL_POSITION
FROM information_schema.key_column_usage
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'users'
  AND CONSTRAINT_NAME = 'PRIMARY'
  AND COLUMN_NAME = 'id'
`).Scan(&primaryOrdinal); err != nil {
		t.Fatalf("query information_schema.key_column_usage primary key: %v", err)
	}
	if primaryOrdinal != 1 {
		t.Fatalf("primary key ordinal = %d, want 1", primaryOrdinal)
	}

	var uniqueName string
	var uniqueOrdinal int
	if err := db.QueryRowContext(ctx, `
SELECT CONSTRAINT_NAME, ORDINAL_POSITION
FROM information_schema.key_column_usage
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'users'
  AND COLUMN_NAME = 'email'
  AND CONSTRAINT_NAME <> 'PRIMARY'
`).Scan(&uniqueName, &uniqueOrdinal); err != nil {
		t.Fatalf("query information_schema.key_column_usage unique key: %v", err)
	}
	if uniqueName == "" || uniqueOrdinal != 1 {
		t.Fatalf("unique key row = name:%q ordinal:%d, want non-empty/1", uniqueName, uniqueOrdinal)
	}

	var primaryConstraintType string
	if err := db.QueryRowContext(ctx, `
SELECT CONSTRAINT_TYPE
FROM information_schema.table_constraints
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'users'
  AND CONSTRAINT_NAME = 'PRIMARY'
`).Scan(&primaryConstraintType); err != nil {
		t.Fatalf("query information_schema.table_constraints primary key: %v", err)
	}
	if primaryConstraintType != "PRIMARY KEY" {
		t.Fatalf("primary constraint type = %q, want PRIMARY KEY", primaryConstraintType)
	}

	var uniqueConstraintType string
	if err := db.QueryRowContext(ctx, `
SELECT CONSTRAINT_TYPE
FROM information_schema.table_constraints
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'users'
  AND CONSTRAINT_NAME = ?
`, uniqueName).Scan(&uniqueConstraintType); err != nil {
		t.Fatalf("query information_schema.table_constraints unique key: %v", err)
	}
	if uniqueConstraintType != "UNIQUE" {
		t.Fatalf("unique constraint type = %q, want UNIQUE", uniqueConstraintType)
	}

	var nonUnique int
	if err := db.QueryRowContext(ctx, `
SELECT NON_UNIQUE
FROM information_schema.statistics
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'users'
  AND COLUMN_NAME = 'email'
`).Scan(&nonUnique); err != nil {
		t.Fatalf("query information_schema.statistics: %v", err)
	}
	if nonUnique != 0 {
		t.Fatalf("email statistics non_unique = %d, want 0", nonUnique)
	}
}

func TestCompatibilityScalarFunctions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	dsn := fmt.Sprintf("user:password@tcp(%s)/customdb?interpolateParams=true&charset=utf8mb4&parseTime=true", server.Addr())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	for query, want := range map[string]string{
		"SELECT DATABASE()":     "customdb",
		"SELECT SCHEMA()":       "customdb",
		"SELECT USER()":         "user@%",
		"SELECT CURRENT_USER()": "user@%",
		"SELECT CURRENT_USER":   "user@%",
	} {
		var got string
		if err := db.QueryRowContext(ctx, query).Scan(&got); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if got != want {
			t.Fatalf("%s = %q, want %q", query, got, want)
		}
	}
	var connectionID int64
	if err := db.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connectionID); err != nil {
		t.Fatalf("select connection_id: %v", err)
	}
	if connectionID == 0 {
		t.Fatal("CONNECTION_ID() returned 0")
	}

	result, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Function", "function@example.com")
	if err != nil {
		t.Fatalf("insert for scalar functions: %v", err)
	}
	insertID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id from result: %v", err)
	}

	var rowCount int64
	if err := db.QueryRowContext(ctx, "SELECT ROW_COUNT()").Scan(&rowCount); err != nil {
		t.Fatalf("select row_count after insert: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("ROW_COUNT() after insert = %d, want 1", rowCount)
	}

	var lastInsertID int64
	if err := db.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()").Scan(&lastInsertID); err != nil {
		t.Fatalf("select last_insert_id: %v", err)
	}
	if lastInsertID != insertID {
		t.Fatalf("LAST_INSERT_ID() = %d, want %d", lastInsertID, insertID)
	}

	if _, err := db.ExecContext(ctx, "UPDATE users SET name = ? WHERE email = ?", "Function Updated", "function@example.com"); err != nil {
		t.Fatalf("update for scalar functions: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT ROW_COUNT()").Scan(&rowCount); err != nil {
		t.Fatalf("select row_count after update: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("ROW_COUNT() after update = %d, want 1", rowCount)
	}
	if err := db.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()").Scan(&lastInsertID); err != nil {
		t.Fatalf("select last_insert_id after update: %v", err)
	}
	if lastInsertID != insertID {
		t.Fatalf("LAST_INSERT_ID() after update = %d, want %d", lastInsertID, insertID)
	}
}

func TestCompatProfileGORMVariables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := testConfig()
	cfg.Compat.Profile = "gorm"
	cfg.Compat.Variables["lower_case_table_names"] = "1"

	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	for query, want := range map[string]string{
		"SELECT @@lower_case_table_names": "1",
		"SELECT @@tx_isolation":           "READ-COMMITTED",
		"SELECT @@time_zone":              "SYSTEM",
		"SELECT @@foreign_key_checks":     "1",
	} {
		var got string
		if err := db.QueryRowContext(ctx, query).Scan(&got); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if got != want {
			t.Fatalf("%s returned %q, want %q", query, got, want)
		}
	}
	assertShowVariable(t, ctx, db, "lower_case_table_names", "1")
	assertShowVariable(t, ctx, db, "tx_isolation", "READ-COMMITTED")
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

func TestSchemaFilesLoadDumpRelativeToConfigFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	dumpPath := filepath.Join(dir, "schema.sql")
	dump := []byte(`
-- MySQL dump
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
DROP TABLE IF EXISTS ` + "`dump_users`" + `;
CREATE TABLE ` + "`dump_users`" + ` (
  ` + "`id`" + ` bigint NOT NULL AUTO_INCREMENT,
  ` + "`email`" + ` varchar(255) NOT NULL,
  ` + "`enabled`" + ` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (` + "`id`" + `),
  UNIQUE KEY ` + "`uniq_dump_users_email`" + ` (` + "`email`" + `)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
LOCK TABLES ` + "`dump_users`" + ` WRITE;
INSERT INTO ` + "`dump_users`" + ` VALUES (100, 'ignored@example.com', 1);
UNLOCK TABLES;
`)
	if err := os.WriteFile(dumpPath, dump, 0o600); err != nil {
		t.Fatal(err)
	}

	configPath := filepath.Join(dir, "mysqlmock.yaml")
	config := []byte(`
version: 1
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
schema_files:
  - schema.sql
seed:
  dump_users:
    - email: "seed@example.com"
`)
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := mysqlmock.CheckConfigFile(ctx, configPath); err != nil {
		t.Fatalf("check config with schema file: %v", err)
	}

	server := mysqlmock.Start(t, mysqlmock.ConfigFile(configPath))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var enabled int
	if err := db.QueryRowContext(ctx, "SELECT enabled FROM dump_users WHERE email = ?", "seed@example.com").Scan(&enabled); err != nil {
		t.Fatalf("select seeded dump user: %v", err)
	}
	if enabled != 0 {
		t.Fatalf("seeded dump user enabled = %d, want default 0", enabled)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO dump_users (email) VALUES (?)", "seed@example.com"); err == nil {
		t.Fatal("expected schema file unique key violation")
	} else if !strings.Contains(err.Error(), "Duplicate entry") {
		t.Fatalf("unexpected schema file unique key error: %v", err)
	}
}

func TestSchemaFileAppliesCompositePrimaryKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	dumpPath := filepath.Join(dir, "schema.sql")
	dump := []byte(`
CREATE TABLE ` + "`composite_links`" + ` (
  ` + "`tenant_id`" + ` bigint unsigned NOT NULL,
  ` + "`id`" + ` bigint NOT NULL AUTO_INCREMENT,
  ` + "`code`" + ` varchar(255) NOT NULL,
  ` + "`locale`" + ` varchar(16) NOT NULL,
  PRIMARY KEY USING BTREE (` + "`id`" + `, ` + "`tenant_id`" + `, ` + "`code`" + `(191), ` + "`locale`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
	if err := os.WriteFile(dumpPath, dump, 0o600); err != nil {
		t.Fatal(err)
	}

	configPath := filepath.Join(dir, "mysqlmock.yaml")
	config := []byte(`
version: 1
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
schema_files:
  - schema.sql
seed:
  composite_links:
    - tenant_id: 7
      id: 1
      code: "news"
      locale: "ja"
`)
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := mysqlmock.CheckConfigFile(ctx, configPath); err != nil {
		t.Fatalf("check config with composite primary key schema: %v", err)
	}

	server := mysqlmock.Start(t, mysqlmock.ConfigFile(configPath))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var pkColumns int
	var maxOrdinal int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*), MAX(ORDINAL_POSITION)
FROM information_schema.key_column_usage
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'composite_links'
  AND CONSTRAINT_NAME = 'PRIMARY'
`).Scan(&pkColumns, &maxOrdinal); err != nil {
		t.Fatalf("query composite primary key metadata: %v", err)
	}
	if pkColumns != 4 || maxOrdinal != 4 {
		t.Fatalf("composite primary key metadata = count:%d max ordinal:%d, want 4/4", pkColumns, maxOrdinal)
	}

	_, err = db.ExecContext(ctx, `
INSERT INTO composite_links (tenant_id, id, code, locale)
VALUES (?, ?, ?, ?)
`, 7, 1, "news", "ja")
	if err == nil {
		t.Fatal("expected composite primary key violation")
	}
	if !strings.Contains(err.Error(), "Duplicate entry") {
		t.Fatalf("unexpected composite primary key error: %v", err)
	}
}

func TestSeedFilesLoadYAMLJSONAndCSVRelativeToConfigFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "yaml_seed.yaml"), []byte(`
seed:
  yaml_users:
    - name: "YAML Alice"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "json_seed.json"), []byte(`{
  "seed": {
    "json_users": [
      {"name": "JSON Bob"}
    ]
  }
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "csv_users.csv"), []byte("name,note\nCSV Carol,\\N\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	configPath := filepath.Join(dir, "mysqlmock.yaml")
	config := []byte(`
version: 1
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
schema:
  - |
    CREATE TABLE yaml_users (
      id INTEGER PRIMARY KEY AUTO_INCREMENT,
      name TEXT NOT NULL
    );
  - |
    CREATE TABLE json_users (
      id INTEGER PRIMARY KEY AUTO_INCREMENT,
      name TEXT NOT NULL
    );
  - |
    CREATE TABLE csv_users (
      id INTEGER PRIMARY KEY AUTO_INCREMENT,
      name TEXT NOT NULL,
      note TEXT NULL
    );
seed_files:
  - yaml_seed.yaml
  - json_seed.json
  - csv_users.csv
seed:
  yaml_users:
    - name: "Inline Dana"
`)
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := mysqlmock.CheckConfigFile(ctx, configPath); err != nil {
		t.Fatalf("check config with seed files: %v", err)
	}

	server := mysqlmock.Start(t, mysqlmock.ConfigFile(configPath))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var yamlCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM yaml_users WHERE name IN (?, ?)", "YAML Alice", "Inline Dana").Scan(&yamlCount); err != nil {
		t.Fatalf("count yaml seed users: %v", err)
	}
	if yamlCount != 2 {
		t.Fatalf("yaml seed user count = %d, want 2", yamlCount)
	}
	var jsonName string
	if err := db.QueryRowContext(ctx, "SELECT name FROM json_users").Scan(&jsonName); err != nil {
		t.Fatalf("select json seed user: %v", err)
	}
	if jsonName != "JSON Bob" {
		t.Fatalf("json seed user = %q, want JSON Bob", jsonName)
	}
	var csvName string
	var csvNote sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT name, note FROM csv_users").Scan(&csvName, &csvNote); err != nil {
		t.Fatalf("select csv seed user: %v", err)
	}
	if csvName != "CSV Carol" || csvNote.Valid {
		t.Fatalf("csv seed row = name:%q note:%#v, want CSV Carol/NULL", csvName, csvNote)
	}
}

func TestSeedFileConfigsLoadTypedCSVWithTableOverride(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	fixturePath, err := filepath.Abs("testdata/typed_seed.csv")
	if err != nil {
		t.Fatalf("resolve typed seed fixture: %v", err)
	}

	configPath := filepath.Join(dir, "mysqlmock.yaml")
	config := []byte(fmt.Sprintf(`
version: 1
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
schema:
  - |
    CREATE TABLE typed_seed_users (
      id INTEGER PRIMARY KEY AUTO_INCREMENT,
      name TEXT NOT NULL,
      active BOOLEAN NOT NULL,
      login_count INTEGER NOT NULL,
      score REAL NOT NULL,
      created_at DATETIME NOT NULL,
      note TEXT NULL
    );
seed_file_configs:
  - path: %q
    table: typed_seed_users
    format: csv
    null_values: ["NULL", "\\N"]
    infer_types: true
`, fixturePath))
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := mysqlmock.CheckConfigFile(ctx, configPath); err != nil {
		t.Fatalf("check config with seed file configs: %v", err)
	}

	server := mysqlmock.Start(t, mysqlmock.ConfigFile(configPath))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var name string
	var active bool
	var loginCount int
	var score float64
	var createdAt time.Time
	var note sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT name, active, login_count, score, created_at, note FROM typed_seed_users").Scan(&name, &active, &loginCount, &score, &createdAt, &note); err != nil {
		t.Fatalf("select typed seed user: %v", err)
	}
	if name != "Typed Alice" || !active || loginCount != 42 || score != 3.5 || createdAt.Format("2006-01-02 15:04:05") != "2026-05-28 10:11:12" || note.Valid {
		t.Fatalf("typed seed row = name:%q active:%v login:%d score:%f created:%s note:%#v", name, active, loginCount, score, createdAt, note)
	}
}

func TestLoadConfigFileRequiresTopLevelFields(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name: "version",
			content: `
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
`,
			wantErr: "version is required",
		},
		{
			name: "server",
			content: `
version: 1
database:
  engine: sqlite
  mode: memory
`,
			wantErr: "server is required",
		},
		{
			name: "database",
			content: `
version: 1
server:
  auth:
    mode: allow_any
`,
			wantErr: "database is required",
		},
		{
			name: "null version",
			content: `
version:
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
`,
			wantErr: "version is required",
		},
		{
			name: "null server",
			content: `
version: 1
server:
database:
  engine: sqlite
  mode: memory
`,
			wantErr: "server is required",
		},
		{
			name: "null database",
			content: `
version: 1
server:
  auth:
    mode: allow_any
database:
`,
			wantErr: "database is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir() + "/mysqlmock.yaml"
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatal(err)
			}

			if _, err := mysqlmock.LoadConfigFile(path); err == nil {
				t.Fatalf("load config succeeded, want error containing %q", tc.wantErr)
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("load config error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadConfigFileAppliesCompatProfile(t *testing.T) {
	path := t.TempDir() + "/mysqlmock.yaml"
	content := []byte(`
version: 1
server:
  mysql_version: "8.0.41-mock"
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
compat:
  profile: gorm
  allow_zero_dates: true
  implicit_defaults: true
  write_validation: basic
  variables:
    lower_case_table_names: "1"
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := mysqlmock.LoadConfigFile(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Compat.Profile != "gorm" {
		t.Fatalf("compat profile = %q, want gorm", cfg.Compat.Profile)
	}
	if !cfg.Compat.AllowZeroDates {
		t.Fatal("compat allow_zero_dates = false, want true")
	}
	if !cfg.Compat.ImplicitDefaults {
		t.Fatal("compat implicit_defaults = false, want true")
	}
	if cfg.Compat.WriteValidation != "basic" {
		t.Fatalf("compat write_validation = %q, want basic", cfg.Compat.WriteValidation)
	}
	for name, want := range map[string]string{
		"version":                "8.0.41-mock",
		"lower_case_table_names": "1",
		"tx_isolation":           "READ-COMMITTED",
		"foreign_key_checks":     "1",
		"character_set_server":   "utf8mb4",
		"transaction_read_only":  "0",
		"character_set_client":   "utf8mb4",
		"transaction_isolation":  "READ-COMMITTED",
	} {
		if got := cfg.Compat.Variables[name]; got != want {
			t.Fatalf("compat variable %s = %q, want %q", name, got, want)
		}
	}
}

func TestFileBackedDatabasePersistsAcrossServerRestarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dbPath := t.TempDir() + "/mysqlmock.sqlite"
	cfg := mysqlmock.DefaultConfig()
	cfg.Backend.Mode = "file"
	cfg.Backend.Path = dbPath
	cfg.Schema = []string{`
CREATE TABLE durable_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL
);`}

	server1, err := mysqlmock.New(mysqlmock.WithConfig(cfg))
	if err != nil {
		t.Fatalf("new first server: %v", err)
	}
	if err := server1.Start(ctx); err != nil {
		t.Fatalf("start first server: %v", err)
	}
	db1, err := sql.Open("mysql", server1.DSN())
	if err != nil {
		t.Fatal(err)
	}
	db1.SetMaxOpenConns(1)
	if _, err := db1.ExecContext(ctx, "INSERT INTO durable_users (name) VALUES (?)", "Persisted"); err != nil {
		t.Fatalf("insert durable row: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close first db: %v", err)
	}
	if err := server1.Close(); err != nil {
		t.Fatalf("close first server: %v", err)
	}

	cfg.Schema = nil
	server2, err := mysqlmock.New(mysqlmock.WithConfig(cfg))
	if err != nil {
		t.Fatalf("new second server: %v", err)
	}
	if err := server2.Start(ctx); err != nil {
		t.Fatalf("start second server: %v", err)
	}
	defer server2.Close()

	db2, err := sql.Open("mysql", server2.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	db2.SetMaxOpenConns(1)

	var name string
	if err := db2.QueryRowContext(ctx, "SELECT name FROM durable_users WHERE id = ?", 1).Scan(&name); err != nil {
		t.Fatalf("select durable row after restart: %v", err)
	}
	if name != "Persisted" {
		t.Fatalf("durable row name = %q, want Persisted", name)
	}
}

func TestSchemaAppliesTranslatedMySQLTokens(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE flags (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  enabled INTEGER NOT NULL DEFAULT TRUE,
  disabled INTEGER NOT NULL DEFAULT FALSE
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	result, err := db.ExecContext(ctx, "INSERT INTO flags DEFAULT VALUES")
	if err != nil {
		t.Fatalf("insert default flags: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if id != 1 {
		t.Fatalf("last insert id = %d, want 1", id)
	}

	var enabled int
	var disabled int
	if err := db.QueryRowContext(ctx, "SELECT enabled, disabled FROM flags WHERE id = ?", id).Scan(&enabled, &disabled); err != nil {
		t.Fatalf("select flags: %v", err)
	}
	if enabled != 1 || disabled != 0 {
		t.Fatalf("flags defaults = enabled:%d disabled:%d, want enabled:1 disabled:0", enabled, disabled)
	}
}

func TestSchemaAppliesCommonMySQLDDLOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE mysql_style_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT COMMENT 'primary id',
  name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'display name',
  quantity INT UNSIGNED NOT NULL DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='mysql style users'
  AUTO_INCREMENT=100 ROW_FORMAT=DYNAMIC KEY_BLOCK_SIZE=8 STATS_PERSISTENT=1;
`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	result, err := db.ExecContext(ctx, "INSERT INTO mysql_style_users (name) VALUES (?)", "Alice")
	if err != nil {
		t.Fatalf("insert mysql style row: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if id != 1 {
		t.Fatalf("last insert id = %d, want 1", id)
	}

	var name string
	var quantity int
	if err := db.QueryRowContext(ctx, "SELECT name, quantity FROM mysql_style_users WHERE id = ?", id).Scan(&name, &quantity); err != nil {
		t.Fatalf("select mysql style row: %v", err)
	}
	if name != "Alice" || quantity != 0 {
		t.Fatalf("mysql style row = name:%q quantity:%d, want Alice/0", name, quantity)
	}
}

func TestSchemaPreservesExplicitDefaultsAndLegacyImplicitDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Compat.ImplicitDefaults = true
	cfg.Compat.AllowZeroDates = true
	cfg.Schema = []string{`
CREATE TABLE legacy_defaults (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  explicit_int INT NOT NULL DEFAULT 999,
  explicit_empty VARCHAR(16) NOT NULL DEFAULT '',
  explicit_zero INT NOT NULL DEFAULT 0,
  implicit_int INT NOT NULL,
  implicit_text VARCHAR(16) NOT NULL,
  implicit_date DATE NOT NULL,
  implicit_datetime DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, `
INSERT INTO legacy_defaults (explicit_int, explicit_empty, explicit_zero)
VALUES (DEFAULT, DEFAULT, DEFAULT)
`); err != nil {
		t.Fatalf("insert explicit defaults with implicit omitted columns: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO legacy_defaults (implicit_int, implicit_text, implicit_date, implicit_datetime)
VALUES (?, ?, ?, ?)
`, nil, nil, nil, nil); err != nil {
		t.Fatalf("insert implicit defaults for NULL values: %v", err)
	}

	rows, err := db.QueryContext(ctx, `
SELECT
  explicit_int,
  explicit_empty,
  explicit_zero,
  implicit_int,
  implicit_text,
  CAST(implicit_date AS TEXT),
  CAST(implicit_datetime AS TEXT)
FROM legacy_defaults
ORDER BY id`)
	if err != nil {
		t.Fatalf("select legacy defaults: %v", err)
	}
	defer rows.Close()

	type defaultRow struct {
		explicitInt      int
		explicitEmpty    string
		explicitZero     int
		implicitInt      int
		implicitText     string
		implicitDate     string
		implicitDateTime string
	}
	got := []defaultRow{}
	for rows.Next() {
		var row defaultRow
		if err := rows.Scan(&row.explicitInt, &row.explicitEmpty, &row.explicitZero, &row.implicitInt, &row.implicitText, &row.implicitDate, &row.implicitDateTime); err != nil {
			t.Fatalf("scan legacy defaults: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read legacy defaults: %v", err)
	}
	want := []defaultRow{
		{explicitInt: 999, explicitEmpty: "", explicitZero: 0, implicitInt: 0, implicitText: "", implicitDate: "0000-00-00", implicitDateTime: "0000-00-00 00:00:00"},
		{explicitInt: 999, explicitEmpty: "", explicitZero: 0, implicitInt: 0, implicitText: "", implicitDate: "0000-00-00", implicitDateTime: "0000-00-00 00:00:00"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy defaults = %#v, want %#v", got, want)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func TestSchemaAppliesPartitionAndZerofillDDL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE partitioned_zerofill_values (
  id INTEGER NOT NULL AUTO_INCREMENT,
  zero_padding INT(5) ZEROFILL NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY HASH(id) PARTITIONS 4;
`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "INSERT INTO partitioned_zerofill_values (zero_padding) VALUES (?)", 42); err != nil {
		t.Fatalf("insert partitioned zerofill row: %v", err)
	}
	var zeroPadding string
	if err := db.QueryRowContext(ctx, "SELECT zero_padding FROM partitioned_zerofill_values WHERE id = ?", 1).Scan(&zeroPadding); err != nil {
		t.Fatalf("select zerofill value: %v", err)
	}
	if zeroPadding != "00042" {
		t.Fatalf("zero_padding = %q, want 00042", zeroPadding)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func TestSchemaAppliesMySQLIndexDDL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{
		`CREATE TABLE indexed_users (
		  id INTEGER PRIMARY KEY AUTO_INCREMENT,
		  name TEXT NOT NULL,
		  email TEXT NOT NULL
		);`,
		`ALTER TABLE indexed_users ADD UNIQUE KEY idx_indexed_users_email (email) USING BTREE;`,
		`CREATE INDEX idx_indexed_users_name USING BTREE ON indexed_users (name);`,
	}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "INSERT INTO indexed_users (name, email) VALUES (?, ?)", "Alice", "alice@example.com"); err != nil {
		t.Fatalf("insert first indexed row: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO indexed_users (name, email) VALUES (?, ?)", "Alice Again", "alice@example.com"); err == nil {
		t.Fatal("expected unique index violation")
	} else if !strings.Contains(err.Error(), "Duplicate entry") {
		t.Fatalf("unexpected unique index error: %v", err)
	}

	var indexCount int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM information_schema.statistics
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'indexed_users'
  AND INDEX_NAME IN ('idx_indexed_users_email', 'idx_indexed_users_name')
`).Scan(&indexCount); err != nil {
		t.Fatalf("query index metadata: %v", err)
	}
	if indexCount != 2 {
		t.Fatalf("index metadata count = %d, want 2", indexCount)
	}
}

func TestSchemaFileAllowsTableScopedDuplicateIndexNames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	if err := os.WriteFile(schemaPath, []byte(`
CREATE TABLE index_scope_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  status VARCHAR(32) NOT NULL,
  KEY idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE index_scope_posts (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  status VARCHAR(32) NOT NULL,
  KEY idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`), 0o644); err != nil {
		t.Fatalf("write schema file: %v", err)
	}

	cfg := mysqlmock.DefaultConfig()
	cfg.SchemaFiles = []string{schemaPath}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	assertIndexCount := func(tableName, indexName string, want int) {
		t.Helper()
		var got int
		if err := db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM information_schema.statistics
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = ?
  AND INDEX_NAME = ?`, tableName, indexName).Scan(&got); err != nil {
			t.Fatalf("query index metadata for %s.%s: %v", tableName, indexName, err)
		}
		if got != want {
			t.Fatalf("index metadata count for %s.%s = %d, want %d", tableName, indexName, got, want)
		}
	}

	assertIndexCount("index_scope_users", "idx_status", 1)
	assertIndexCount("index_scope_posts", "idx_status", 1)

	if _, err := db.ExecContext(ctx, "ALTER TABLE index_scope_posts DROP INDEX idx_status"); err != nil {
		t.Fatalf("drop table-scoped duplicate index: %v", err)
	}
	assertIndexCount("index_scope_users", "idx_status", 1)
	assertIndexCount("index_scope_posts", "idx_status", 0)

	if _, err := db.ExecContext(ctx, "ALTER TABLE index_scope_users RENAME INDEX idx_status TO idx_state"); err != nil {
		t.Fatalf("rename table-scoped duplicate index: %v", err)
	}
	assertIndexCount("index_scope_users", "idx_status", 0)
	assertIndexCount("index_scope_users", "idx_state", 1)

	mysqlmock.AssertNoUnsupported(t, server)
}

func TestSchemaAppliesTiDBDDL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE update_patterns (
  id BIGINT PRIMARY KEY /*T![clustered_index] CLUSTERED */ /*T![auto_rand] AUTO_RANDOM(5) */,
  code VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  channel_id BIGINT NOT NULL,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uniq_update_patterns_code (code),
  KEY idx_update_patterns_channel_id (channel_id(20))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_RANDOM_BASE=100;
`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	result, err := db.ExecContext(ctx, "INSERT INTO update_patterns (code, channel_id) VALUES (?, ?)", "daily", 10)
	if err != nil {
		t.Fatalf("insert TiDB style row: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if id != 1 {
		t.Fatalf("last insert id = %d, want 1", id)
	}

	if _, err := db.ExecContext(ctx, "INSERT INTO update_patterns (code, channel_id) VALUES (?, ?)", "daily", 20); err == nil {
		t.Fatal("expected unique key violation")
	} else if !strings.Contains(err.Error(), "Duplicate entry") {
		t.Fatalf("unexpected unique key error: %v", err)
	}

	var indexCount int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM information_schema.statistics
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'update_patterns'
  AND INDEX_NAME IN ('uniq_update_patterns_code', 'idx_update_patterns_channel_id')
`).Scan(&indexCount); err != nil {
		t.Fatalf("query TiDB index metadata: %v", err)
	}
	if indexCount != 2 {
		t.Fatalf("TiDB index metadata count = %d, want 2", indexCount)
	}
}

func TestOnDuplicateKeyUpdateExecutesInsertAndUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE upsert_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  login_count INTEGER NOT NULL DEFAULT 0,
  updated_at DATETIME NULL
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	insertResult, err := db.ExecContext(ctx, `
INSERT INTO upsert_users (email, name, login_count, updated_at)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  login_count = login_count + VALUES(login_count),
  updated_at = VALUES(updated_at)
`, "alice@example.com", "Alice", 1, "2026-05-28 10:00:00")
	if err != nil {
		t.Fatalf("insert upsert row: %v", err)
	}
	insertAffected, err := insertResult.RowsAffected()
	if err != nil {
		t.Fatalf("insert rows affected: %v", err)
	}
	if insertAffected != 1 {
		t.Fatalf("insert rows affected = %d, want 1", insertAffected)
	}
	insertID, err := insertResult.LastInsertId()
	if err != nil {
		t.Fatalf("insert last insert id: %v", err)
	}
	if insertID != 1 {
		t.Fatalf("insert last insert id = %d, want 1", insertID)
	}

	updateResult, err := db.ExecContext(ctx, `
INSERT INTO upsert_users (email, name, login_count, updated_at)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  login_count = login_count + VALUES(login_count),
  updated_at = VALUES(updated_at)
`, "alice@example.com", "Alice Updated", 2, "2026-05-28 11:00:00")
	if err != nil {
		t.Fatalf("update upsert row: %v", err)
	}
	updateAffected, err := updateResult.RowsAffected()
	if err != nil {
		t.Fatalf("update rows affected: %v", err)
	}
	if updateAffected != 2 {
		t.Fatalf("update rows affected = %d, want 2", updateAffected)
	}
	updateID, err := updateResult.LastInsertId()
	if err != nil {
		t.Fatalf("update last insert id: %v", err)
	}
	if updateID != 1 {
		t.Fatalf("update last insert id = %d, want 1", updateID)
	}

	var name string
	var loginCount int
	var updatedAt string
	if err := db.QueryRowContext(ctx, "SELECT name, login_count, updated_at FROM upsert_users WHERE email = ?", "alice@example.com").Scan(&name, &loginCount, &updatedAt); err != nil {
		t.Fatalf("select upsert row: %v", err)
	}
	if name != "Alice Updated" || loginCount != 3 || !strings.Contains(updatedAt, "2026-05-28") {
		t.Fatalf("upsert row = name:%q login_count:%d updated_at:%q, want updated values", name, loginCount, updatedAt)
	}

	sameResult, err := db.ExecContext(ctx, `
INSERT INTO upsert_users (email, name, login_count, updated_at)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  updated_at = VALUES(updated_at)
`, "alice@example.com", "Alice Updated", 99, updatedAt)
	if err != nil {
		t.Fatalf("same-value upsert row: %v", err)
	}
	sameAffected, err := sameResult.RowsAffected()
	if err != nil {
		t.Fatalf("same-value rows affected: %v", err)
	}
	if sameAffected != 0 {
		t.Fatalf("same-value rows affected = %d, want 0", sameAffected)
	}

	var rowCount int
	if err := db.QueryRowContext(ctx, "SELECT ROW_COUNT()").Scan(&rowCount); err != nil {
		t.Fatalf("select row count: %v", err)
	}
	if rowCount != 0 {
		t.Fatalf("ROW_COUNT() after same-value upsert = %d, want 0", rowCount)
	}
}

func TestOnDuplicateKeyUpdateHandlesBulkRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE bulk_links (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  channel_id INTEGER NOT NULL,
  pattern_id INTEGER NOT NULL,
  label TEXT NOT NULL,
  UNIQUE KEY uniq_bulk_links (channel_id, pattern_id)
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "INSERT INTO bulk_links (channel_id, pattern_id, label) VALUES (?, ?, ?)", 1, 10, "old"); err != nil {
		t.Fatalf("seed bulk link: %v", err)
	}

	result, err := db.ExecContext(ctx, `
INSERT INTO bulk_links (channel_id, pattern_id, label)
VALUES (?, ?, ?), (?, ?, ?)
ON DUPLICATE KEY UPDATE label = VALUES(label)
`, 1, 10, "updated", 1, 20, "new")
	if err != nil {
		t.Fatalf("bulk upsert: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("bulk rows affected: %v", err)
	}
	if affected != 3 {
		t.Fatalf("bulk rows affected = %d, want 3", affected)
	}

	rows, err := db.QueryContext(ctx, "SELECT pattern_id, label FROM bulk_links WHERE channel_id = ? ORDER BY pattern_id", 1)
	if err != nil {
		t.Fatalf("select bulk links: %v", err)
	}
	defer rows.Close()

	got := map[int]string{}
	for rows.Next() {
		var patternID int
		var label string
		if err := rows.Scan(&patternID, &label); err != nil {
			t.Fatalf("scan bulk link: %v", err)
		}
		got[patternID] = label
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read bulk links: %v", err)
	}
	want := map[int]string{10: "updated", 20: "new"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bulk links = %#v, want %#v", got, want)
	}
	if unsupported := server.Unsupported(); len(unsupported) != 0 {
		t.Fatalf("unsupported queries = %#v, want none", unsupported)
	}
}

func TestInsertCompatibilityParseCacheBindsArgumentsPerExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE cached_insert_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  login_count INTEGER NOT NULL DEFAULT 0
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	upsertSQL := `
INSERT INTO cached_insert_users (email, name, login_count)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  login_count = login_count + VALUES(login_count)
`
	if _, err := db.ExecContext(ctx, upsertSQL, "cache-a@example.com", "Cache A", 1); err != nil {
		t.Fatalf("first cached upsert: %v", err)
	}
	if _, err := db.ExecContext(ctx, upsertSQL, "cache-b@example.com", "Cache B", 2); err != nil {
		t.Fatalf("second cached upsert: %v", err)
	}
	if _, err := db.ExecContext(ctx, upsertSQL, "cache-a@example.com", "Cache A Updated", 3); err != nil {
		t.Fatalf("third cached upsert: %v", err)
	}

	ignoreSQL := `
INSERT IGNORE INTO cached_insert_users (email, name, login_count)
VALUES (?, ?, ?)
`
	if _, err := db.ExecContext(ctx, ignoreSQL, "cache-a@example.com", "Ignored", 99); err != nil {
		t.Fatalf("first cached insert ignore: %v", err)
	}
	if _, err := db.ExecContext(ctx, ignoreSQL, "cache-c@example.com", "Cache C", 4); err != nil {
		t.Fatalf("second cached insert ignore: %v", err)
	}

	rows, err := db.QueryContext(ctx, "SELECT email, name, login_count FROM cached_insert_users ORDER BY email")
	if err != nil {
		t.Fatalf("select cached insert rows: %v", err)
	}
	defer rows.Close()

	got := map[string]struct {
		name       string
		loginCount int
	}{}
	for rows.Next() {
		var email string
		var name string
		var loginCount int
		if err := rows.Scan(&email, &name, &loginCount); err != nil {
			t.Fatalf("scan cached insert row: %v", err)
		}
		got[email] = struct {
			name       string
			loginCount int
		}{name: name, loginCount: loginCount}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read cached insert rows: %v", err)
	}
	want := map[string]struct {
		name       string
		loginCount int
	}{
		"cache-a@example.com": {name: "Cache A Updated", loginCount: 4},
		"cache-b@example.com": {name: "Cache B", loginCount: 2},
		"cache-c@example.com": {name: "Cache C", loginCount: 4},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cached insert rows = %#v, want %#v", got, want)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func TestOnDuplicateKeyUpdateHandlesActiveRecordAliasSyntax(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE ar_alias_upserts (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  login_count INTEGER NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "INSERT INTO ar_alias_upserts (email, name, login_count, updated_at) VALUES (?, ?, ?, ?)", "alice@example.com", "Alice", 1, "2026-05-28 10:00:00"); err != nil {
		t.Fatalf("seed alias upsert row: %v", err)
	}

	result, err := db.ExecContext(ctx, `
INSERT INTO `+"`ar_alias_upserts`"+` (`+"`email`"+`, `+"`name`"+`, `+"`login_count`"+`, `+"`updated_at`"+`)
VALUES (?, ?, ?, ?) AS `+"`ar_alias_upserts_values`"+`
ON DUPLICATE KEY UPDATE
  `+"`name`"+` = `+"`ar_alias_upserts_values`"+`.`+"`name`"+`,
  `+"`login_count`"+` = `+"`ar_alias_upserts`"+`.`+"`login_count`"+` + `+"`ar_alias_upserts_values`"+`.`+"`login_count`"+`,
  `+"`updated_at`"+` = CASE WHEN `+"`ar_alias_upserts`"+`.`+"`name`"+` <=> `+"`ar_alias_upserts_values`"+`.`+"`name`"+` THEN `+"`ar_alias_upserts`"+`.`+"`updated_at`"+` ELSE CURRENT_TIMESTAMP END
`, "alice@example.com", "Alice Updated", 2, "2026-05-28 11:00:00")
	if err != nil {
		t.Fatalf("ActiveRecord alias upsert: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("alias upsert rows affected: %v", err)
	}
	if affected != 2 {
		t.Fatalf("alias upsert rows affected = %d, want 2", affected)
	}

	var name string
	var loginCount int
	var updatedAt string
	if err := db.QueryRowContext(ctx, "SELECT name, login_count, updated_at FROM ar_alias_upserts WHERE email = ?", "alice@example.com").Scan(&name, &loginCount, &updatedAt); err != nil {
		t.Fatalf("select alias upsert row: %v", err)
	}
	if name != "Alice Updated" || loginCount != 3 || updatedAt == "2026-05-28 10:00:00" {
		t.Fatalf("alias upsert row = name:%q login_count:%d updated_at:%q, want updated values", name, loginCount, updatedAt)
	}

	skipResult, err := db.ExecContext(ctx, `
INSERT INTO `+"`ar_alias_upserts`"+` (`+"`email`"+`, `+"`name`"+`, `+"`login_count`"+`)
VALUES (?, ?, ?) AS `+"`ar_alias_upserts_values`"+`
ON DUPLICATE KEY UPDATE `+"`email`"+` = `+"`ar_alias_upserts`"+`.`+"`email`"+`
`, "alice@example.com", "Ignored", 99)
	if err != nil {
		t.Fatalf("ActiveRecord alias skip duplicate upsert: %v", err)
	}
	skipAffected, err := skipResult.RowsAffected()
	if err != nil {
		t.Fatalf("alias skip rows affected: %v", err)
	}
	if skipAffected != 0 {
		t.Fatalf("alias skip rows affected = %d, want 0", skipAffected)
	}
}

func TestOnDuplicateKeyUpdateHandlesDefaultValues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE default_upserts (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  code VARCHAR(64) NOT NULL UNIQUE,
  enabled INTEGER NOT NULL DEFAULT 0,
  title TEXT NOT NULL DEFAULT 'untitled',
  note TEXT NULL DEFAULT 'seed',
  touched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  optional_text TEXT NULL
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, `
INSERT INTO default_upserts (code, enabled, title, note, optional_text)
VALUES (?, ?, ?, ?, ?)
`, "existing", 1, "custom", "custom note", "custom optional"); err != nil {
		t.Fatalf("seed default upsert row: %v", err)
	}

	result, err := db.ExecContext(ctx, `
INSERT INTO default_upserts (id, code, enabled, title, note, touched_at, optional_text)
VALUES (DEFAULT, ?, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT),
       (DEFAULT, ?, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT)
ON DUPLICATE KEY UPDATE
  enabled = VALUES(enabled),
  title = VALUES(title),
  note = VALUES(note),
  touched_at = VALUES(touched_at),
  optional_text = VALUES(optional_text)
`, "existing", "new")
	if err != nil {
		t.Fatalf("default upsert: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("default upsert rows affected: %v", err)
	}
	if affected != 3 {
		t.Fatalf("default upsert rows affected = %d, want 3", affected)
	}
	lastID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("default upsert last insert id: %v", err)
	}
	if lastID != 1 {
		t.Fatalf("default upsert last insert id = %d, want 1", lastID)
	}

	rows, err := db.QueryContext(ctx, `
SELECT code, enabled, title, note, touched_at, optional_text
FROM default_upserts
ORDER BY code
`)
	if err != nil {
		t.Fatalf("select default upserts: %v", err)
	}
	defer rows.Close()

	type row struct {
		code     string
		enabled  int
		title    string
		note     sql.NullString
		touched  sql.NullString
		optional sql.NullString
	}
	got := []row{}
	for rows.Next() {
		var item row
		if err := rows.Scan(&item.code, &item.enabled, &item.title, &item.note, &item.touched, &item.optional); err != nil {
			t.Fatalf("scan default upsert: %v", err)
		}
		got = append(got, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read default upserts: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("default upsert row count = %d, want 2: %#v", len(got), got)
	}
	for _, item := range got {
		if item.enabled != 0 || item.title != "untitled" || !item.note.Valid || item.note.String != "seed" || !item.touched.Valid || item.optional.Valid {
			t.Fatalf("default upsert row = %#v, want defaults applied", item)
		}
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func TestInsertIgnoreSkipsDuplicateRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE ignore_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL
);`}
	cfg.Seed = map[string][]map[string]any{
		"ignore_users": {
			{"email": "existing@example.com", "name": "Existing"},
		},
	}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	result, err := db.ExecContext(ctx, `
INSERT IGNORE INTO ignore_users (email, name)
VALUES (?, ?), (?, ?)
`, "existing@example.com", "Ignored", "new@example.com", "New")
	if err != nil {
		t.Fatalf("insert ignore users: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("insert ignore rows affected: %v", err)
	}
	if affected != 1 {
		t.Fatalf("insert ignore rows affected = %d, want 1", affected)
	}
	lastID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("insert ignore last insert id: %v", err)
	}
	if lastID == 0 {
		t.Fatal("insert ignore last insert id = 0, want inserted row id")
	}

	stmt, err := db.PrepareContext(ctx, `
INSERT IGNORE INTO ignore_users (email, name)
VALUES (?, ?), (?, ?)
`)
	if err != nil {
		t.Fatalf("prepare insert ignore: %v", err)
	}
	defer stmt.Close()
	preparedResult, err := stmt.ExecContext(ctx, "new@example.com", "Ignored Again", "prepared@example.com", "Prepared")
	if err != nil {
		t.Fatalf("prepared insert ignore users: %v", err)
	}
	preparedAffected, err := preparedResult.RowsAffected()
	if err != nil {
		t.Fatalf("prepared insert ignore rows affected: %v", err)
	}
	if preparedAffected != 1 {
		t.Fatalf("prepared insert ignore rows affected = %d, want 1", preparedAffected)
	}

	rows, err := db.QueryContext(ctx, "SELECT email, name FROM ignore_users ORDER BY email")
	if err != nil {
		t.Fatalf("select ignore users: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var email string
		var name string
		if err := rows.Scan(&email, &name); err != nil {
			t.Fatalf("scan ignore user: %v", err)
		}
		got[email] = name
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read ignore users: %v", err)
	}
	want := map[string]string{
		"existing@example.com": "Existing",
		"new@example.com":      "New",
		"prepared@example.com": "Prepared",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ignore users = %#v, want %#v", got, want)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func TestReplaceIntoReplacesDuplicateRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE replace_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL
);`}
	cfg.Seed = map[string][]map[string]any{
		"replace_users": {
			{"email": "existing@example.com", "name": "Existing"},
		},
	}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	result, err := db.ExecContext(ctx, `
REPLACE INTO replace_users (email, name)
VALUES (?, ?), (?, ?)
`, "existing@example.com", "Replaced", "new@example.com", "New")
	if err != nil {
		t.Fatalf("replace users: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("replace rows affected: %v", err)
	}
	if affected != 3 {
		t.Fatalf("replace rows affected = %d, want 3", affected)
	}
	lastID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("replace last insert id: %v", err)
	}
	if lastID == 0 {
		t.Fatal("replace last insert id = 0, want inserted row id")
	}

	stmt, err := db.PrepareContext(ctx, `
REPLACE INTO replace_users (email, name)
VALUES (?, ?)
`)
	if err != nil {
		t.Fatalf("prepare replace: %v", err)
	}
	defer stmt.Close()
	preparedResult, err := stmt.ExecContext(ctx, "new@example.com", "Prepared Replaced")
	if err != nil {
		t.Fatalf("prepared replace user: %v", err)
	}
	preparedAffected, err := preparedResult.RowsAffected()
	if err != nil {
		t.Fatalf("prepared replace rows affected: %v", err)
	}
	if preparedAffected != 2 {
		t.Fatalf("prepared replace rows affected = %d, want 2", preparedAffected)
	}

	rows, err := db.QueryContext(ctx, "SELECT email, name FROM replace_users ORDER BY email")
	if err != nil {
		t.Fatalf("select replace users: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var email string
		var name string
		if err := rows.Scan(&email, &name); err != nil {
			t.Fatalf("scan replace user: %v", err)
		}
		got[email] = name
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read replace users: %v", err)
	}
	want := map[string]string{
		"existing@example.com": "Replaced",
		"new@example.com":      "Prepared Replaced",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replace users = %#v, want %#v", got, want)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func TestUnsupportedInsertIgnoreSuggestion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE ignore_select_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL UNIQUE
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "INSERT IGNORE INTO ignore_select_users SELECT 1, 'unsupported@example.com'"); err == nil {
		t.Fatal("expected unsupported INSERT IGNORE SELECT error")
	}
	unsupported := server.Unsupported()
	if len(unsupported) != 1 {
		t.Fatalf("unsupported count = %d, want 1: %#v", len(unsupported), unsupported)
	}
	if unsupported[0].Suggestion == "" || !strings.Contains(unsupported[0].Suggestion, "INSERT IGNORE INTO ignore_select_users SELECT") {
		t.Fatalf("unsupported suggestion = %q, want INSERT IGNORE rule suggestion", unsupported[0].Suggestion)
	}
}

func TestBunStyleSQLCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{
		`CREATE TABLE bun_users (
		  id INTEGER PRIMARY KEY AUTO_INCREMENT,
		  name VARCHAR(255) NOT NULL,
		  created_at DATETIME NOT NULL,
		  deleted_at DATETIME NULL,
		  note TEXT NULL
		);`,
		`CREATE TABLE bun_profiles (
		  id INTEGER PRIMARY KEY AUTO_INCREMENT,
		  user_id INTEGER NOT NULL,
		  label TEXT NOT NULL,
		  UNIQUE KEY uniq_bun_profiles_user_id (user_id)
		);`,
	}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	createdAt := time.Date(2026, 5, 28, 12, 34, 56, 0, time.UTC)
	userResult, err := db.ExecContext(ctx, "INSERT INTO bun_users (name, created_at, deleted_at, note) VALUES (?, ?, ?, ?)", "Alice", createdAt, nil, nil)
	if err != nil {
		t.Fatalf("insert bun user: %v", err)
	}
	userID, err := userResult.LastInsertId()
	if err != nil {
		t.Fatalf("bun user last insert id: %v", err)
	}
	if userID != 1 {
		t.Fatalf("bun user last insert id = %d, want 1", userID)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO bun_profiles (user_id, label) VALUES (?, ?)", userID, "primary"); err != nil {
		t.Fatalf("insert bun profile: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM bun_users WHERE id IN (?, ?)", userID, 999).Scan(&count); err != nil {
		t.Fatalf("bun count with IN: %v", err)
	}
	if count != 1 {
		t.Fatalf("bun count = %d, want 1", count)
	}

	var gotUserID int64
	var gotName string
	var gotCreatedAt time.Time
	var gotDeletedAt sql.NullTime
	var gotNote sql.NullString
	var gotProfileLabel string
	if err := db.QueryRowContext(ctx, `
SELECT
  u.id AS user__id,
  u.name AS user__name,
  u.created_at AS user__created_at,
  u.deleted_at AS user__deleted_at,
  u.note AS user__note,
  p.label AS profile__label
FROM bun_users AS u
JOIN bun_profiles AS p ON p.user_id = u.id
WHERE u.id IN (?, ?)
`, userID, 999).Scan(&gotUserID, &gotName, &gotCreatedAt, &gotDeletedAt, &gotNote, &gotProfileLabel); err != nil {
		t.Fatalf("bun relation select: %v", err)
	}
	if gotUserID != userID || gotName != "Alice" || gotProfileLabel != "primary" {
		t.Fatalf("bun relation row = id:%d name:%q label:%q", gotUserID, gotName, gotProfileLabel)
	}
	if gotCreatedAt.Format("2006-01-02 15:04:05") != createdAt.Format("2006-01-02 15:04:05") {
		t.Fatalf("bun created_at = %s, want %s", gotCreatedAt, createdAt)
	}
	if gotDeletedAt.Valid {
		t.Fatalf("bun deleted_at = %#v, want NULL", gotDeletedAt)
	}
	if gotNote.Valid {
		t.Fatalf("bun note = %#v, want NULL", gotNote)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func TestMySQLFunctionCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE function_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  payload TEXT NOT NULL,
  score TEXT NOT NULL,
  nickname TEXT NULL
);`}
	cfg.Seed = map[string][]map[string]any{
		"function_users": {
			{
				"first_name": "Ada",
				"last_name":  "Lovelace",
				"created_at": "2026-05-28 10:11:12",
				"payload":    `{"role":"admin","level":7}`,
				"score":      "42",
				"nickname":   nil,
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

	var fullName string
	var formatted string
	var score int
	var nickname string
	var roleJSON string
	var role string
	if err := db.QueryRowContext(ctx, `
SELECT
  CONCAT(first_name, ' ', last_name),
  DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'),
  CAST(score AS SIGNED),
  COALESCE(nickname, IFNULL(NULL, 'none')),
  JSON_EXTRACT(payload, '$.role'),
  JSON_UNQUOTE(JSON_EXTRACT(payload, '$.role'))
FROM function_users
`).Scan(&fullName, &formatted, &score, &nickname, &roleJSON, &role); err != nil {
		t.Fatalf("select MySQL functions: %v", err)
	}
	if fullName != "Ada Lovelace" || formatted != "2026-05-28 10:11:12" || score != 42 || nickname != "none" || roleJSON != `"admin"` || role != "admin" {
		t.Fatalf("function row = name:%q formatted:%q score:%d nickname:%q role_json:%q role:%q", fullName, formatted, score, nickname, roleJSON, role)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func TestActiveRecordStyleMySQLCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{
		`CREATE TABLE ar_users (
		  id INTEGER PRIMARY KEY AUTO_INCREMENT,
		  email VARCHAR(255) NOT NULL UNIQUE,
		  name VARCHAR(255) NOT NULL DEFAULT 'anonymous',
		  nickname VARCHAR(255) NOT NULL DEFAULT '',
		  login_count INT NOT NULL DEFAULT '999',
		  active TINYINT(1) NOT NULL DEFAULT 1,
		  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX index_ar_users_on_name ON ar_users (name);`,
		`CREATE INDEX index_ar_users_on_email_prefix ON ar_users (email(10));`,
		`CREATE INDEX index_ar_users_on_lower_name ON ar_users ((lower(name)));`,
		`CREATE INDEX index_ar_users_on_active_invisible ON ar_users (active) INVISIBLE;`,
		`CREATE TABLE schema_migrations (
		  version VARCHAR(255) NOT NULL PRIMARY KEY
		);`,
		`CREATE TABLE ar_internal_metadata (
		  ` + "`key`" + ` VARCHAR(255) NOT NULL PRIMARY KEY,
		  value VARCHAR(255),
		  created_at DATETIME NOT NULL,
		  updated_at DATETIME NOT NULL
		);`,
		`CREATE TABLE ar_parent_records (
		  id INTEGER PRIMARY KEY AUTO_INCREMENT
		);`,
		`CREATE TABLE ar_child_records (
		  id INTEGER PRIMARY KEY AUTO_INCREMENT,
		  parent_id INTEGER NOT NULL,
		  FOREIGN KEY (parent_id) REFERENCES ar_parent_records(id)
		);`,
	}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "SET @@SESSION.sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), @@SESSION.wait_timeout = '2147483'"); err != nil {
		t.Fatalf("set ActiveRecord session variables: %v", err)
	}
	if _, err := db.ExecContext(ctx, "SET NAMES utf8mb4, @@SESSION.wait_timeout = '123'"); err != nil {
		t.Fatalf("set names with ActiveRecord session variable: %v", err)
	}
	var waitTimeout string
	if err := db.QueryRowContext(ctx, "SELECT @@wait_timeout").Scan(&waitTimeout); err != nil {
		t.Fatalf("select wait_timeout: %v", err)
	}
	if waitTimeout != "123" {
		t.Fatalf("wait_timeout = %q, want 123", waitTimeout)
	}
	if _, err := db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatalf("disable foreign key checks: %v", err)
	}
	var foreignKeyChecks string
	if err := db.QueryRowContext(ctx, "SELECT @@FOREIGN_KEY_CHECKS").Scan(&foreignKeyChecks); err != nil {
		t.Fatalf("select foreign_key_checks: %v", err)
	}
	if foreignKeyChecks != "0" {
		t.Fatalf("foreign_key_checks = %q, want 0", foreignKeyChecks)
	}
	if _, err := db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatalf("enable foreign key checks: %v", err)
	}

	var charsetDatabase string
	if err := db.QueryRowContext(ctx, "SELECT @@character_set_database").Scan(&charsetDatabase); err != nil {
		t.Fatalf("select character_set_database: %v", err)
	}
	if charsetDatabase != "utf8mb4" {
		t.Fatalf("character_set_database = %q, want utf8mb4", charsetDatabase)
	}

	var lockResult int
	if err := db.QueryRowContext(ctx, "SELECT GET_LOCK('ar_schema_migrations', 0)").Scan(&lockResult); err != nil {
		t.Fatalf("get advisory lock: %v", err)
	}
	if lockResult != 1 {
		t.Fatalf("GET_LOCK result = %d, want 1", lockResult)
	}
	if err := db.QueryRowContext(ctx, "SELECT RELEASE_LOCK('ar_schema_migrations')").Scan(&lockResult); err != nil {
		t.Fatalf("release advisory lock: %v", err)
	}
	if lockResult != 1 {
		t.Fatalf("RELEASE_LOCK result = %d, want 1", lockResult)
	}

	fields, err := db.QueryContext(ctx, "SHOW FULL FIELDS FROM `ar_users`")
	if err != nil {
		t.Fatalf("show full fields: %v", err)
	}
	defer fields.Close()

	type fieldRow struct {
		field      string
		typ        string
		collation  sql.NullString
		null       string
		key        string
		def        sql.NullString
		extra      string
		privileges string
		comment    string
	}
	fieldByName := map[string]fieldRow{}
	for fields.Next() {
		var row fieldRow
		if err := fields.Scan(&row.field, &row.typ, &row.collation, &row.null, &row.key, &row.def, &row.extra, &row.privileges, &row.comment); err != nil {
			t.Fatalf("scan full field: %v", err)
		}
		fieldByName[row.field] = row
	}
	if err := fields.Err(); err != nil {
		t.Fatalf("read full fields: %v", err)
	}
	if fieldByName["id"].key != "PRI" || fieldByName["id"].extra != "auto_increment" {
		t.Fatalf("id field metadata = %#v, want primary auto_increment", fieldByName["id"])
	}
	if fieldByName["email"].key != "UNI" || fieldByName["email"].null != "NO" {
		t.Fatalf("email field metadata = %#v, want unique not-null", fieldByName["email"])
	}
	if fieldByName["name"].key != "MUL" || !fieldByName["name"].def.Valid || fieldByName["name"].def.String != "anonymous" {
		t.Fatalf("name field metadata = %#v, want indexed default", fieldByName["name"])
	}
	if !fieldByName["nickname"].def.Valid || fieldByName["nickname"].def.String != "" {
		t.Fatalf("nickname field metadata = %#v, want empty string default", fieldByName["nickname"])
	}
	if !fieldByName["login_count"].def.Valid || fieldByName["login_count"].def.String != "999" {
		t.Fatalf("login_count field metadata = %#v, want quoted numeric default normalized", fieldByName["login_count"])
	}

	var likeField string
	var likeType string
	var likeCollation sql.NullString
	var likeNull string
	var likeKey string
	var likeDefault sql.NullString
	var likeExtra string
	var likePrivileges string
	var likeComment string
	if err := db.QueryRowContext(ctx, "SHOW FULL FIELDS FROM `ar_users` LIKE 'email'").Scan(&likeField, &likeType, &likeCollation, &likeNull, &likeKey, &likeDefault, &likeExtra, &likePrivileges, &likeComment); err != nil {
		t.Fatalf("show full fields like: %v", err)
	}
	if likeField != "email" || likeKey != "UNI" {
		t.Fatalf("SHOW FULL FIELDS LIKE returned field=%q key=%q, want email/UNI", likeField, likeKey)
	}

	keyRows, err := db.QueryContext(ctx, "SHOW KEYS FROM `ar_users`")
	if err != nil {
		t.Fatalf("show keys: %v", err)
	}
	defer keyRows.Close()

	keys := map[string][]string{}
	subParts := map[string]int{}
	visibleByKey := map[string]string{}
	expressions := map[string]string{}
	for keyRows.Next() {
		var table string
		var nonUnique int
		var keyName string
		var seqInIndex int
		var columnName sql.NullString
		var collation sql.NullString
		var cardinality sql.NullString
		var subPart sql.NullString
		var packed sql.NullString
		var nullValue sql.NullString
		var indexType string
		var comment string
		var indexComment string
		var visible string
		var expression sql.NullString
		if err := keyRows.Scan(&table, &nonUnique, &keyName, &seqInIndex, &columnName, &collation, &cardinality, &subPart, &packed, &nullValue, &indexType, &comment, &indexComment, &visible, &expression); err != nil {
			t.Fatalf("scan key row: %v", err)
		}
		_ = table
		_ = nonUnique
		_ = seqInIndex
		_ = collation
		_ = cardinality
		_ = packed
		_ = nullValue
		_ = comment
		_ = indexComment
		if indexType != "BTREE" || visible != "YES" {
			if keyName != "index_ar_users_on_active_invisible" || visible != "NO" {
				t.Fatalf("key row type=%q visible=%q, want BTREE/YES or invisible test index", indexType, visible)
			}
		}
		if columnName.Valid {
			keys[keyName] = append(keys[keyName], columnName.String)
		}
		if subPart.Valid {
			value, err := strconv.Atoi(subPart.String)
			if err != nil {
				t.Fatalf("parse sub_part %q: %v", subPart.String, err)
			}
			subParts[keyName] = value
		}
		visibleByKey[keyName] = visible
		if expression.Valid {
			expressions[keyName] = expression.String
		}
	}
	if err := keyRows.Err(); err != nil {
		t.Fatalf("read key rows: %v", err)
	}
	if !reflect.DeepEqual(keys["PRIMARY"], []string{"id"}) {
		t.Fatalf("primary key columns = %#v, want id", keys["PRIMARY"])
	}
	if !reflect.DeepEqual(keys["index_ar_users_on_name"], []string{"name"}) {
		t.Fatalf("name index columns = %#v, want name", keys["index_ar_users_on_name"])
	}
	if subParts["index_ar_users_on_email_prefix"] != 10 {
		t.Fatalf("email prefix sub_part = %d, want 10", subParts["index_ar_users_on_email_prefix"])
	}
	if expressions["index_ar_users_on_lower_name"] != "lower(name)" {
		t.Fatalf("lower name expression = %q, want lower(name)", expressions["index_ar_users_on_lower_name"])
	}
	if visibleByKey["index_ar_users_on_active_invisible"] != "NO" {
		t.Fatalf("invisible index visibility = %q, want NO", visibleByKey["index_ar_users_on_active_invisible"])
	}

	if _, err := db.ExecContext(ctx, "ALTER TABLE `ar_users` ALTER INDEX `index_ar_users_on_active_invisible` VISIBLE"); err != nil {
		t.Fatalf("alter index visible: %v", err)
	}
	visibleByKey = map[string]string{}
	visibleRows, err := db.QueryContext(ctx, "SHOW KEYS FROM `ar_users`")
	if err != nil {
		t.Fatalf("show keys after visibility change: %v", err)
	}
	for visibleRows.Next() {
		var table string
		var nonUnique int
		var keyName string
		var seqInIndex int
		var columnName sql.NullString
		var collation sql.NullString
		var cardinality sql.NullString
		var subPart sql.NullString
		var packed sql.NullString
		var nullValue sql.NullString
		var indexType string
		var comment string
		var indexComment string
		var visible string
		var expression sql.NullString
		if err := visibleRows.Scan(&table, &nonUnique, &keyName, &seqInIndex, &columnName, &collation, &cardinality, &subPart, &packed, &nullValue, &indexType, &comment, &indexComment, &visible, &expression); err != nil {
			t.Fatalf("scan visible key row: %v", err)
		}
		visibleByKey[keyName] = visible
	}
	if err := visibleRows.Err(); err != nil {
		t.Fatalf("read visible key rows: %v", err)
	}
	if err := visibleRows.Close(); err != nil {
		t.Fatalf("close visible key rows: %v", err)
	}
	if visibleByKey["index_ar_users_on_active_invisible"] != "YES" {
		t.Fatalf("index visibility after ALTER = %q, want YES", visibleByKey["index_ar_users_on_active_invisible"])
	}

	var tableName string
	var createSQL string
	if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE `ar_users`").Scan(&tableName, &createSQL); err != nil {
		t.Fatalf("show create table: %v", err)
	}
	if tableName != "ar_users" || !strings.Contains(strings.ToUpper(createSQL), "CREATE TABLE") {
		t.Fatalf("SHOW CREATE TABLE = table:%q sql:%q, want ar_users CREATE TABLE", tableName, createSQL)
	}
	if !strings.Contains(createSQL, "ENGINE=InnoDB") || !strings.Contains(createSQL, "DEFAULT CHARSET=utf8mb4") {
		t.Fatalf("SHOW CREATE TABLE sql = %q, want MySQL table options", createSQL)
	}
	if !strings.Contains(createSQL, "TINYINT(1)") || !strings.Contains(createSQL, "ON UPDATE CURRENT_TIMESTAMP") {
		t.Fatalf("SHOW CREATE TABLE sql = %q, want configured MySQL schema text", createSQL)
	}

	if _, err := db.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES (?)", "20260528000000"); err != nil {
		t.Fatalf("insert schema migration: %v", err)
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT `schema_migrations`.`version` FROM `schema_migrations` ORDER BY `schema_migrations`.`version` ASC").Scan(&version); err != nil {
		t.Fatalf("select schema migration version: %v", err)
	}
	if version != "20260528000000" {
		t.Fatalf("schema migration version = %q, want 20260528000000", version)
	}

	if _, err := db.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
		t.Fatalf("set transaction isolation: %v", err)
	}

	var tableComment string
	if err := db.QueryRowContext(ctx, `
SELECT table_comment
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name = 'ar_users'
`).Scan(&tableComment); err != nil {
		t.Fatalf("select ActiveRecord table_comment metadata: %v", err)
	}
	if tableComment != "" {
		t.Fatalf("table_comment = %q, want empty string", tableComment)
	}

	checkRows, err := db.QueryContext(ctx, `
SELECT cc.constraint_name AS name, cc.check_clause AS expression
FROM information_schema.check_constraints cc
JOIN information_schema.table_constraints tc USING (constraint_schema, constraint_name)
WHERE tc.table_schema = DATABASE()
  AND tc.table_name = 'ar_users'
  AND cc.constraint_schema = DATABASE()
`)
	if err != nil {
		t.Fatalf("select ActiveRecord check constraint metadata: %v", err)
	}
	if checkRows.Next() {
		t.Fatal("check constraint metadata returned rows, want none")
	}
	if err := checkRows.Err(); err != nil {
		t.Fatalf("read check constraint metadata: %v", err)
	}
	if err := checkRows.Close(); err != nil {
		t.Fatalf("close check constraint metadata: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin foreign_key_checks transaction: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("disable foreign key checks inside transaction: %v", err)
	}
	var txForeignKeyChecks string
	if err := tx.QueryRowContext(ctx, "SELECT @@FOREIGN_KEY_CHECKS").Scan(&txForeignKeyChecks); err != nil {
		_ = tx.Rollback()
		t.Fatalf("select tx foreign_key_checks: %v", err)
	}
	if txForeignKeyChecks != "0" {
		_ = tx.Rollback()
		t.Fatalf("tx foreign_key_checks = %q, want 0", txForeignKeyChecks)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO ar_child_records (parent_id) VALUES (?)", 999); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert with foreign_key_checks disabled inside transaction: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback foreign_key_checks transaction: %v", err)
	}

	mysqlmock.AssertNoUnsupported(t, server)
}

func TestActiveRecordAdvisoryLockCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(mysqlmock.DefaultConfig()))
	db1, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	db1.SetMaxOpenConns(1)

	db2, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	db2.SetMaxOpenConns(1)

	var got int
	if err := db1.QueryRowContext(ctx, "SELECT GET_LOCK('ar_schema_migrations', 0)").Scan(&got); err != nil {
		t.Fatalf("db1 get advisory lock: %v", err)
	}
	if got != 1 {
		t.Fatalf("db1 GET_LOCK = %d, want 1", got)
	}
	if err := db2.QueryRowContext(ctx, "SELECT GET_LOCK('ar_schema_migrations', 0)").Scan(&got); err != nil {
		t.Fatalf("db2 get advisory lock: %v", err)
	}
	if got != 0 {
		t.Fatalf("db2 GET_LOCK while held = %d, want 0", got)
	}
	if err := db2.QueryRowContext(ctx, "SELECT RELEASE_LOCK('ar_schema_migrations')").Scan(&got); err != nil {
		t.Fatalf("db2 release advisory lock: %v", err)
	}
	if got != 0 {
		t.Fatalf("db2 RELEASE_LOCK for another connection = %d, want 0", got)
	}
	if err := db1.QueryRowContext(ctx, "SELECT RELEASE_LOCK('ar_schema_migrations')").Scan(&got); err != nil {
		t.Fatalf("db1 release advisory lock: %v", err)
	}
	if got != 1 {
		t.Fatalf("db1 RELEASE_LOCK = %d, want 1", got)
	}
	if err := db2.QueryRowContext(ctx, "SELECT GET_LOCK('ar_schema_migrations', 0)").Scan(&got); err != nil {
		t.Fatalf("db2 get released advisory lock: %v", err)
	}
	if got != 1 {
		t.Fatalf("db2 GET_LOCK after release = %d, want 1", got)
	}

	mysqlmock.AssertNoUnsupported(t, server)
}

func TestShowCreateTableInvalidatesConfiguredDDLAfterRuntimeAlter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE show_create_originals (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var tableName string
	var createSQL string
	if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE `show_create_originals`").Scan(&tableName, &createSQL); err != nil {
		t.Fatalf("show original create table: %v", err)
	}
	if !strings.Contains(createSQL, "ON UPDATE CURRENT_TIMESTAMP") || !strings.Contains(createSQL, "COLLATE=utf8mb4_bin") {
		t.Fatalf("original SHOW CREATE TABLE = %q, want configured MySQL DDL", createSQL)
	}

	if _, err := db.ExecContext(ctx, "ALTER TABLE `show_create_originals` ADD COLUMN `nickname` VARCHAR(64) DEFAULT 'none'"); err != nil {
		t.Fatalf("alter table add nickname: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE `show_create_originals`").Scan(&tableName, &createSQL); err != nil {
		t.Fatalf("show altered create table: %v", err)
	}
	if !strings.Contains(createSQL, "nickname") {
		t.Fatalf("altered SHOW CREATE TABLE = %q, want runtime SQLite definition after invalidation", createSQL)
	}
	if strings.Contains(createSQL, "ON UPDATE CURRENT_TIMESTAMP") {
		t.Fatalf("altered SHOW CREATE TABLE = %q, should not return stale configured DDL", createSQL)
	}
}

func TestMySQLDDLCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE ddl_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL
);`}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "ALTER TABLE ddl_users ADD COLUMN display_name VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT 'none'"); err != nil {
		t.Fatalf("alter add column: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE ddl_users ADD INDEX idx_ddl_users_name (name) USING BTREE"); err != nil {
		t.Fatalf("alter add index: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE ddl_users RENAME INDEX idx_ddl_users_name TO idx_ddl_users_name_new"); err != nil {
		t.Fatalf("alter rename index: %v", err)
	}
	assertShowKeyColumns(t, ctx, db, "ddl_users", "idx_ddl_users_name_new", []string{"name"})

	if _, err := db.ExecContext(ctx, "ALTER TABLE ddl_users DROP INDEX idx_ddl_users_name_new"); err != nil {
		t.Fatalf("alter drop index: %v", err)
	}
	assertShowKeyColumns(t, ctx, db, "ddl_users", "idx_ddl_users_name_new", nil)

	if _, err := db.ExecContext(ctx, "CREATE INDEX idx_ddl_users_display_name ON ddl_users (display_name)"); err != nil {
		t.Fatalf("create display name index: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DROP INDEX idx_ddl_users_display_name ON ddl_users"); err != nil {
		t.Fatalf("drop index on table: %v", err)
	}
	assertShowKeyColumns(t, ctx, db, "ddl_users", "idx_ddl_users_display_name", nil)

	if _, err := db.ExecContext(ctx, "ALTER TABLE ddl_users CHANGE COLUMN display_name nickname VARCHAR(20) NOT NULL"); err != nil {
		t.Fatalf("alter change column: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE ddl_users MODIFY COLUMN nickname VARCHAR(40) NOT NULL"); err != nil {
		t.Fatalf("alter modify column: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO ddl_users (email, name, nickname) VALUES (?, ?, ?)", "ddl@example.com", "DDL", "Nick"); err != nil {
		t.Fatalf("insert after ddl changes: %v", err)
	}

	if _, err := db.ExecContext(ctx, "RENAME TABLE ddl_users TO ddl_people"); err != nil {
		t.Fatalf("rename table: %v", err)
	}
	var nickname string
	if err := db.QueryRowContext(ctx, "SELECT nickname FROM ddl_people WHERE email = ?", "ddl@example.com").Scan(&nickname); err != nil {
		t.Fatalf("select renamed table: %v", err)
	}
	if nickname != "Nick" {
		t.Fatalf("renamed table nickname = %q, want Nick", nickname)
	}
	if _, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS `teardown_db`"); err != nil {
		t.Fatalf("drop database teardown: %v", err)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}

func assertShowKeyColumns(t *testing.T, ctx context.Context, db *sql.DB, tableName, keyName string, want []string) {
	t.Helper()

	rows, err := db.QueryContext(ctx, "SHOW KEYS FROM "+tableName)
	if err != nil {
		t.Fatalf("show keys from %s: %v", tableName, err)
	}
	defer rows.Close()

	got := []string{}
	for rows.Next() {
		var table string
		var nonUnique int
		var currentKey string
		var seqInIndex int
		var columnName string
		var collation sql.NullString
		var cardinality sql.NullString
		var subPart sql.NullString
		var packed sql.NullString
		var nullValue sql.NullString
		var indexType string
		var comment string
		var indexComment string
		var visible string
		var expression sql.NullString
		if err := rows.Scan(&table, &nonUnique, &currentKey, &seqInIndex, &columnName, &collation, &cardinality, &subPart, &packed, &nullValue, &indexType, &comment, &indexComment, &visible, &expression); err != nil {
			t.Fatalf("scan show keys: %v", err)
		}
		if currentKey == keyName {
			got = append(got, columnName)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read show keys: %v", err)
	}
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SHOW KEYS %s columns = %#v, want %#v", keyName, got, want)
	}
}

func TestMemoryDatabaseSharedFalseIsolatesConnections(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := testConfig()
	cfg.Backend.Shared = false
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))

	db1, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	db1.SetMaxOpenConns(1)

	db2, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	db2.SetMaxOpenConns(1)

	if _, err := db1.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Private", "private@example.com"); err != nil {
		t.Fatalf("insert private row: %v", err)
	}
	assertUserCount(t, ctx, db1, 3)
	assertUserCount(t, ctx, db2, 2)

	var name string
	err = db2.QueryRowContext(ctx, "SELECT name FROM users WHERE email = ?", "private@example.com").Scan(&name)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected private row to be hidden from second connection, got name=%q err=%v", name, err)
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
	if len(server.Queries()) == 0 {
		t.Fatal("expected query events before reset")
	}

	if err := server.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if len(server.Queries()) != 0 {
		t.Fatalf("query event count after reset = %d, want 0", len(server.Queries()))
	}
	assertUserCount(t, ctx, db, 2)
	if len(server.Unsupported()) != 0 {
		t.Fatalf("unsupported count after reset = %d, want 0", len(server.Unsupported()))
	}
	if err := db.QueryRowContext(ctx, "SELECT 'reset_once'").Scan(new(string)); err == nil {
		t.Fatal("expected once rule error after reset")
	}
	result, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "After Reset", "after-reset@example.com")
	if err != nil {
		t.Fatalf("insert after reset: %v", err)
	}
	lastID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id after reset: %v", err)
	}
	if lastID != 3 {
		t.Fatalf("last insert id after reset = %d, want 3", lastID)
	}
}

func TestServerResetUsesPreparedSchemaAndSeedCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	if err := os.WriteFile(schemaPath, []byte(`
CREATE TABLE cached_users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  email TEXT NOT NULL UNIQUE
);
`), 0o600); err != nil {
		t.Fatal(err)
	}
	seedPath := filepath.Join(dir, "seed.yaml")
	if err := os.WriteFile(seedPath, []byte(`
seed:
  cached_users:
    - email: "seed@example.com"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "mysqlmock.yaml")
	if err := os.WriteFile(configPath, []byte(`
version: 1
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
schema_files:
  - schema.sql
seed_files:
  - seed.yaml
`), 0o600); err != nil {
		t.Fatal(err)
	}

	server := mysqlmock.Start(t, mysqlmock.ConfigFile(configPath))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if err := os.Remove(schemaPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(seedPath); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, "INSERT INTO cached_users (email) VALUES (?)", "runtime@example.com"); err != nil {
		t.Fatalf("insert runtime row: %v", err)
	}
	if err := server.Reset(ctx); err != nil {
		t.Fatalf("fast reset with removed config files: %v", err)
	}
	assertTableCount(t, ctx, db, "cached_users", 1)

	if _, err := db.ExecContext(ctx, "CREATE TABLE runtime_only (id INTEGER PRIMARY KEY AUTO_INCREMENT)"); err != nil {
		t.Fatalf("create runtime table: %v", err)
	}
	if err := server.Reset(ctx); err != nil {
		t.Fatalf("full reset with removed config files: %v", err)
	}
	assertTableCount(t, ctx, db, "cached_users", 1)
	var tableName string
	err = db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'runtime_only'").Scan(&tableName)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("runtime table after full reset = %q, err=%v, want no rows", tableName, err)
	}
}

func TestUpsertMetadataCacheInvalidatesAfterSchemaChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := mysqlmock.DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE cache_upserts (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  code TEXT NOT NULL,
  count INTEGER NOT NULL DEFAULT 0
);`}
	cfg.Seed = map[string][]map[string]any{
		"cache_upserts": {
			{"id": 1, "code": "a", "count": 1},
		},
	}
	server := mysqlmock.Start(t, mysqlmock.WithConfig(cfg))
	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, `
INSERT INTO cache_upserts (id, code, count)
VALUES (1, 'a', 2)
ON DUPLICATE KEY UPDATE count = VALUES(count)
`); err != nil {
		t.Fatalf("initial primary-key upsert: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE UNIQUE INDEX uniq_cache_upserts_code ON cache_upserts (code)"); err != nil {
		t.Fatalf("create unique index after cache warmup: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO cache_upserts (id, code, count)
VALUES (2, 'a', 3)
ON DUPLICATE KEY UPDATE count = VALUES(count)
`); err != nil {
		t.Fatalf("upsert after unique index schema change: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT count FROM cache_upserts WHERE id = 1").Scan(&count); err != nil {
		t.Fatalf("select upserted count: %v", err)
	}
	if count != 3 {
		t.Fatalf("upserted count = %d, want 3", count)
	}
	assertTableCount(t, ctx, db, "cache_upserts", 1)
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

func TestQuerySnapshotRecordsQueriesWithoutLogWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))
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
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("select one: %v", err)
	}

	queries := server.Queries()
	if len(queries) < 2 {
		t.Fatalf("query event count = %d, want at least 2", len(queries))
	}
	foundVersion := false
	foundSelectOne := false
	for _, query := range queries {
		if query.Route == "compat" && query.SQL == "SELECT VERSION()" {
			foundVersion = true
		}
		if query.Route == "sqlite" && query.SQL == "SELECT 1" {
			foundSelectOne = true
		}
	}
	if !foundVersion {
		t.Fatalf("SELECT VERSION query event not found in %+v", queries)
	}
	if !foundSelectOne {
		t.Fatalf("SELECT 1 query event not found in %+v", queries)
	}

	snapshot, err := server.QuerySnapshotJSON()
	if err != nil {
		t.Fatalf("QuerySnapshotJSON: %v", err)
	}
	got := string(snapshot)
	for _, want := range []string{`"route": "compat"`, `"route": "sqlite"`, `"sql": "SELECT 1"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("query snapshot %s does not contain %q", got, want)
		}
	}
	if strings.Contains(got, "connection_id") {
		t.Fatalf("query snapshot contains connection_id: %s", got)
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
	for _, want := range []string{
		"type: result_set",
		`name: "@@session.unknown_variable"`,
		`- ["TODO"]`,
	} {
		if !strings.Contains(unsupported[0].Suggestion, want) {
			t.Fatalf("unsupported suggestion %q does not contain %q", unsupported[0].Suggestion, want)
		}
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

func TestCompatProfileValidation(t *testing.T) {
	cfg := mysqlmock.DefaultConfig()
	cfg.Compat.Profile = " GORM "
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate gorm compat profile: %v", err)
	}

	cfg = mysqlmock.DefaultConfig()
	cfg.Compat.Profile = "unknown"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unsupported compat profile error")
	}
}

func TestWriteValidationModeValidation(t *testing.T) {
	for _, mode := range []string{"strict", "basic", "off", " BASIC "} {
		cfg := mysqlmock.DefaultConfig()
		cfg.Compat.WriteValidation = mode
		if err := cfg.Validate(); err != nil {
			t.Fatalf("validate write_validation %q: %v", mode, err)
		}
	}

	cfg := mysqlmock.DefaultConfig()
	cfg.Compat.WriteValidation = "unknown"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unsupported write_validation mode error")
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

	assertTableCount(t, ctx, db, "users", want)
}

func assertTableCount(t *testing.T, ctx context.Context, db *sql.DB, table string, want int) {
	t.Helper()

	var got int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteMySQLTestIdent(table)).Scan(&got); err != nil {
		t.Fatalf("select %s count: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}

func quoteMySQLTestIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
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
