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
  ` + "`id`" + ` BIGINT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM(5) */,
  ` + "`email`" + ` varchar(255) NOT NULL,
  ` + "`enabled`" + ` tinyint(1) NOT NULL DEFAULT 0,
  UNIQUE KEY ` + "`uniq_dump_users_email`" + ` (` + "`email`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
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
