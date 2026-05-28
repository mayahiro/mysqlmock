package basic_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"

	_ "github.com/go-sql-driver/mysql"
)

type user struct {
	ID    int64
	Name  string
	Email string
}

type userRepository struct {
	db *sql.DB
}

func (r userRepository) FindByEmail(ctx context.Context, email string) (user, error) {
	var u user
	err := r.db.QueryRowContext(ctx, `
SELECT id, name, email
FROM users
WHERE email = ?
`, email).Scan(&u.ID, &u.Name, &u.Email)
	return u, err
}

func (r userRepository) FindByID(ctx context.Context, id int64) (user, error) {
	var u user
	err := r.db.QueryRowContext(ctx, `
SELECT id, name, email
FROM users
WHERE id = ?
`, id).Scan(&u.ID, &u.Name, &u.Email)
	return u, err
}

func (r userRepository) Create(ctx context.Context, name, email string) (user, error) {
	res, err := r.db.ExecContext(ctx, `
INSERT INTO users (name, email)
VALUES (?, ?)
`, name, email)
	if err != nil {
		return user{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return user{}, err
	}
	return r.FindByID(ctx, id)
}

func TestUserRepositoryWithMysqlmock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mysqlmock.Start(t, mysqlmock.ConfigFile("config.yaml"))

	db, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping mysqlmock: %v", err)
	}

	repo := userRepository{db: db}

	alice, err := repo.FindByEmail(ctx, "alice@example.com")
	if err != nil {
		t.Fatalf("find seed user: %v", err)
	}
	if alice.Name != "Alice" {
		t.Fatalf("seed user name = %q, want Alice", alice.Name)
	}

	created, err := repo.Create(ctx, "Carol", "carol@example.com")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if created.ID == 0 || created.Name != "Carol" {
		t.Fatalf("created user = %#v, want non-zero ID and name Carol", created)
	}

	if err := server.Reset(ctx); err != nil {
		t.Fatalf("reset mysqlmock: %v", err)
	}

	_, err = repo.FindByEmail(ctx, "carol@example.com")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("created user after reset error = %v, want sql.ErrNoRows", err)
	}

	snapshotDir := t.TempDir()
	if err := mysqlmock.WriteQuerySnapshot(filepath.Join(snapshotDir, "queries.golden.json"), server.Queries()); err != nil {
		t.Fatalf("write query snapshot: %v", err)
	}
	if err := mysqlmock.WriteUnsupportedSnapshot(filepath.Join(snapshotDir, "unsupported.golden.json"), server.Unsupported()); err != nil {
		t.Fatalf("write unsupported snapshot: %v", err)
	}
	mysqlmock.AssertNoUnsupported(t, server)
}
