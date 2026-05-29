package mysqlmock_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"

	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLFunctionCompatibilityUDFs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var randValue float64
	if err := db.QueryRowContext(ctx, "SELECT RAND()").Scan(&randValue); err != nil {
		t.Fatalf("SELECT RAND(): %v", err)
	}
	if randValue < 0 || randValue >= 1 {
		t.Fatalf("RAND() = %v, want value in [0, 1)", randValue)
	}

	var firstSeededRand float64
	var secondSeededRand float64
	if err := db.QueryRowContext(ctx, "SELECT RAND(3), RAND(3)").Scan(&firstSeededRand, &secondSeededRand); err != nil {
		t.Fatalf("SELECT RAND(seed): %v", err)
	}
	if firstSeededRand != secondSeededRand {
		t.Fatalf("RAND(seed) values = %v and %v, want repeatable values for equal seeds", firstSeededRand, secondSeededRand)
	}

	var findInSet int
	var findInSetMissing int
	var nullFindInSet sql.NullInt64
	if err := db.QueryRowContext(ctx, `
SELECT
  FIND_IN_SET('b', 'a,b,c'),
  FIND_IN_SET('x', 'a,b,c'),
  FIND_IN_SET(NULL, 'a,b,c')
`).Scan(&findInSet, &findInSetMissing, &nullFindInSet); err != nil {
		t.Fatalf("SELECT FIND_IN_SET(): %v", err)
	}
	if findInSet != 2 || findInSetMissing != 0 || nullFindInSet.Valid {
		t.Fatalf("FIND_IN_SET results = %d, %d, %#v, want 2, 0, NULL", findInSet, findInSetMissing, nullFindInSet)
	}

	var fieldString int
	var fieldNumber int
	var fieldNull int
	if err := db.QueryRowContext(ctx, `
SELECT
  FIELD('b', 'a', 'b', 'c'),
  FIELD(20, 10, 20, 30),
  FIELD(NULL, 'a', 'b')
`).Scan(&fieldString, &fieldNumber, &fieldNull); err != nil {
		t.Fatalf("SELECT FIELD(): %v", err)
	}
	if fieldString != 2 || fieldNumber != 2 || fieldNull != 0 {
		t.Fatalf("FIELD results = %d, %d, %d, want 2, 2, 0", fieldString, fieldNumber, fieldNull)
	}

	var matched int
	var notMatched int
	var nullRegexp sql.NullInt64
	if err := db.QueryRowContext(ctx, `
SELECT
  'Alice' REGEXP '^A',
  'Bob' REGEXP '^A',
  NULL REGEXP '^A'
`).Scan(&matched, &notMatched, &nullRegexp); err != nil {
		t.Fatalf("SELECT REGEXP: %v", err)
	}
	if matched != 1 || notMatched != 0 || nullRegexp.Valid {
		t.Fatalf("REGEXP results = %d, %d, %#v, want 1, 0, NULL", matched, notMatched, nullRegexp)
	}
}

func TestMySQLFunctionCompatibilityUDFsInRepositoryQueries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.WithConfig(testConfig()))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE FIND_IN_SET(name, ?) > 0 ORDER BY id", "Carol,Alice").Scan(&name); err != nil {
		t.Fatalf("query with FIND_IN_SET(): %v", err)
	}
	if name != "Alice" {
		t.Fatalf("FIND_IN_SET query returned %q, want Alice", name)
	}

	if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE name REGEXP ? ORDER BY id", "^A").Scan(&name); err != nil {
		t.Fatalf("query with REGEXP: %v", err)
	}
	if name != "Alice" {
		t.Fatalf("REGEXP query returned %q, want Alice", name)
	}

	if err := db.QueryRowContext(ctx, "SELECT name FROM users ORDER BY FIELD(name, 'Bob', 'Alice') LIMIT 1").Scan(&name); err != nil {
		t.Fatalf("query with FIELD(): %v", err)
	}
	if name != "Bob" {
		t.Fatalf("FIELD order returned %q, want Bob", name)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users ORDER BY RAND()").Scan(&count); err != nil {
		t.Fatalf("query with ORDER BY RAND(): %v", err)
	}
	if count != 2 {
		t.Fatalf("ORDER BY RAND() count = %d, want 2", count)
	}
}
