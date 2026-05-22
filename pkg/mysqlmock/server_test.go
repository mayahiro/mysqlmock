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

func TestServerWithGoSQLDriverMySQLMVP0(t *testing.T) {
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

	if _, err := db.PrepareContext(ctx, "SELECT name FROM users WHERE id = ?"); err == nil {
		t.Fatal("expected prepared statement to be unsupported")
	} else if !strings.Contains(err.Error(), "Prepared statements are not supported") {
		t.Fatalf("unexpected prepare error: %v", err)
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
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := mysqlmock.CheckConfigFile(context.Background(), path); err != nil {
		t.Fatalf("check config: %v", err)
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
