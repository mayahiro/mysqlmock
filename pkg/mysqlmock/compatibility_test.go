package mysqlmock_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"
)

func TestRealMySQLCompatibilityScenario(t *testing.T) {
	realDSN := os.Getenv("MYSQLMOCK_REAL_MYSQL_DSN")
	if realDSN == "" {
		t.Skip("set MYSQLMOCK_REAL_MYSQL_DSN to compare mysqlmock with a real MySQL database")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := fmt.Sprintf("mysqlmock_compat_%d", time.Now().UnixNano())

	realDB, err := sql.Open("mysql", realDSN)
	if err != nil {
		t.Fatalf("open real MySQL: %v", err)
	}
	defer realDB.Close()
	realDB.SetMaxOpenConns(1)

	server := mysqlmock.Start(t, mysqlmock.WithConfig(mysqlmock.DefaultConfig()))
	mockDB, err := sql.Open("mysql", server.DSN())
	if err != nil {
		t.Fatalf("open mysqlmock: %v", err)
	}
	defer mockDB.Close()
	mockDB.SetMaxOpenConns(1)

	realObs, err := runCompatibilityScenario(ctx, realDB, table)
	if err != nil {
		t.Fatalf("real MySQL scenario: %v", err)
	}
	mockObs, err := runCompatibilityScenario(ctx, mockDB, table)
	if err != nil {
		t.Fatalf("mysqlmock scenario: %v", err)
	}

	if !reflect.DeepEqual(mockObs, realObs) {
		t.Fatalf("compatibility observation mismatch\nreal:      %+v\nmysqlmock: %+v", realObs, mockObs)
	}
}

type compatibilityObservation struct {
	FirstInsertID     int64
	FirstRowsAffected int64
	Names             []string
	RollbackCount     int
	DuplicateCode     uint16
	DuplicateSQLState string
}

func runCompatibilityScenario(ctx context.Context, db *sql.DB, table string) (compatibilityObservation, error) {
	quotedTable := quoteMySQLCompatIdent(table)
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quotedTable)

	createSQL := fmt.Sprintf(`CREATE TEMPORARY TABLE %s (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL UNIQUE
)`, quotedTable)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		return compatibilityObservation{}, fmt.Errorf("create table: %w", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quotedTable)

	result, err := db.ExecContext(ctx, "INSERT INTO "+quotedTable+" (name) VALUES (?)", "Alice")
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("insert alice: %w", err)
	}
	firstInsertID, err := result.LastInsertId()
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("last insert id: %w", err)
	}
	firstRowsAffected, err := result.RowsAffected()
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("rows affected: %w", err)
	}

	_, err = db.ExecContext(ctx, "INSERT INTO "+quotedTable+" (name) VALUES (?)", "Alice")
	if err == nil {
		return compatibilityObservation{}, errors.New("duplicate insert succeeded")
	}
	duplicateCode, duplicateSQLState := mysqlErrorCodeState(err)
	if duplicateCode == 0 || duplicateSQLState == "" {
		return compatibilityObservation{}, fmt.Errorf("duplicate insert did not return MySQL error metadata: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("begin tx: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+quotedTable+" (name) VALUES (?)", "Rolled Back"); err != nil {
		_ = tx.Rollback()
		return compatibilityObservation{}, fmt.Errorf("insert rollback row: %w", err)
	}
	if err := tx.Rollback(); err != nil {
		return compatibilityObservation{}, fmt.Errorf("rollback tx: %w", err)
	}

	var rollbackCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quotedTable+" WHERE name = ?", "Rolled Back").Scan(&rollbackCount); err != nil {
		return compatibilityObservation{}, fmt.Errorf("select rollback count: %w", err)
	}

	rows, err := db.QueryContext(ctx, "SELECT name FROM "+quotedTable+" ORDER BY id")
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("select names: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return compatibilityObservation{}, fmt.Errorf("scan name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return compatibilityObservation{}, fmt.Errorf("read names: %w", err)
	}

	return compatibilityObservation{
		FirstInsertID:     firstInsertID,
		FirstRowsAffected: firstRowsAffected,
		Names:             names,
		RollbackCount:     rollbackCount,
		DuplicateCode:     duplicateCode,
		DuplicateSQLState: duplicateSQLState,
	}, nil
}

func mysqlErrorCodeState(err error) (uint16, string) {
	var mysqlErr *mysqlDriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return 0, ""
	}
	return mysqlErr.Number, string(mysqlErr.SQLState[:])
}

func quoteMySQLCompatIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}
