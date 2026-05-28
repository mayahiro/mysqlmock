package mysqlmock_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
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

func TestRealTiDBCompatibilityScenario(t *testing.T) {
	realDSN := os.Getenv("MYSQLMOCK_REAL_TIDB_DSN")
	if realDSN == "" {
		t.Skip("set MYSQLMOCK_REAL_TIDB_DSN to compare mysqlmock with a real TiDB database")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	table := fmt.Sprintf("mysqlmock_tidb_compat_%d", time.Now().UnixNano())

	realDB, err := sql.Open("mysql", realDSN)
	if err != nil {
		t.Fatalf("open real TiDB: %v", err)
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
		t.Fatalf("real TiDB scenario: %v", err)
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
	FirstInsertID        int64
	FirstRowsAffected    int64
	UpsertRowsAffected   int64
	UpsertLastInsertID   int64
	InsertIgnoreAffected int64
	InsertIgnoreLastID   int64
	ReplaceRowsAffected  int64
	ReplaceLastInsertID  int64
	Rows                 []string
	RollbackCount        int
	DuplicateCode        uint16
	DuplicateSQLState    string
	DuplicateMentionsKey bool
	DataTooLongCode      uint16
	DataTooLongSQLState  string
	DataTooLongMessage   string
	ShowIDKey            string
	ShowNameKey          string
	ShowCreateHasEngine  bool
	ShowCreateHasCharset bool
	ShowNamePrefixSub    int
	DateFormatValue      string
	JSONExtractValue     string
	ForeignKeyChecksTx   string
	ForeignKeyInsertOK   bool
	AlteredColumnName    string
}

func runCompatibilityScenario(ctx context.Context, db *sql.DB, table string) (compatibilityObservation, error) {
	quotedTable := quoteMySQLCompatIdent(table)
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quotedTable)

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL UNIQUE,
  visits INT NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  payload JSON NULL
)`, quotedTable)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		return compatibilityObservation{}, fmt.Errorf("create table: %w", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quotedTable)

	prefixIndex := quoteMySQLCompatIdent(table + "_name_prefix")
	if _, err := db.ExecContext(ctx, "CREATE INDEX "+prefixIndex+" ON "+quotedTable+" (name(8))"); err != nil {
		return compatibilityObservation{}, fmt.Errorf("create prefix index: %w", err)
	}

	result, err := db.ExecContext(ctx, "INSERT INTO "+quotedTable+" (name, visits, created_at, payload) VALUES (?, ?, ?, ?)", "Alice", 0, "2026-05-28 10:11:12", `{"role":"admin"}`)
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
	duplicateMentionsKey := strings.Contains(err.Error(), "Duplicate entry")

	_, err = db.ExecContext(ctx, "INSERT INTO "+quotedTable+" (name) VALUES (?)", strings.Repeat("x", 65))
	if err == nil {
		return compatibilityObservation{}, errors.New("data-too-long insert succeeded")
	}
	dataTooLongCode, dataTooLongSQLState, dataTooLongMessage := mysqlErrorCodeStateMessage(err)
	if dataTooLongCode == 0 || dataTooLongSQLState == "" {
		return compatibilityObservation{}, fmt.Errorf("data-too-long insert did not return MySQL error metadata: %w", err)
	}

	upsertResult, err := db.ExecContext(ctx, `
INSERT INTO `+quotedTable+` (name, visits)
VALUES (?, ?), (?, ?)
ON DUPLICATE KEY UPDATE visits = visits + VALUES(visits)
`, "Alice", 2, "Bob", 1)
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("upsert rows: %w", err)
	}
	upsertRowsAffected, err := upsertResult.RowsAffected()
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("upsert rows affected: %w", err)
	}
	upsertLastInsertID, err := upsertResult.LastInsertId()
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("upsert last insert id: %w", err)
	}

	ignoreResult, err := db.ExecContext(ctx, `
INSERT IGNORE INTO `+quotedTable+` (name, visits)
VALUES (?, ?), (?, ?)
`, "Alice", 99, "Carol", 5)
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("insert ignore rows: %w", err)
	}
	insertIgnoreAffected, err := ignoreResult.RowsAffected()
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("insert ignore rows affected: %w", err)
	}
	insertIgnoreLastID, err := ignoreResult.LastInsertId()
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("insert ignore last insert id: %w", err)
	}

	replaceResult, err := db.ExecContext(ctx, `
REPLACE INTO `+quotedTable+` (name, visits)
VALUES (?, ?), (?, ?)
`, "Bob", 9, "Dave", 4)
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("replace rows: %w", err)
	}
	replaceRowsAffected, err := replaceResult.RowsAffected()
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("replace rows affected: %w", err)
	}
	replaceLastInsertID, err := replaceResult.LastInsertId()
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("replace last insert id: %w", err)
	}

	showIDKey, showNameKey, err := compatibilityShowFieldKeys(ctx, db, quotedTable)
	if err != nil {
		return compatibilityObservation{}, err
	}
	showCreateHasEngine, showCreateHasCharset, err := compatibilityShowCreateTableOptions(ctx, db, quotedTable)
	if err != nil {
		return compatibilityObservation{}, err
	}
	showNamePrefixSub, err := compatibilityShowKeySubPart(ctx, db, quotedTable, table+"_name_prefix")
	if err != nil {
		return compatibilityObservation{}, err
	}

	var dateFormatValue string
	var jsonExtractValue string
	if err := db.QueryRowContext(ctx, "SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'), JSON_EXTRACT(payload, '$.role') FROM "+quotedTable+" WHERE name = ?", "Alice").Scan(&dateFormatValue, &jsonExtractValue); err != nil {
		return compatibilityObservation{}, fmt.Errorf("select function compatibility values: %w", err)
	}

	foreignKeyChecksTx, foreignKeyInsertOK, err := runForeignKeyChecksTransactionScenario(ctx, db, table)
	if err != nil {
		return compatibilityObservation{}, err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("begin tx: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+quotedTable+" (name, visits) VALUES (?, ?)", "Rolled Back", 0); err != nil {
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

	rows, err := db.QueryContext(ctx, "SELECT name, visits FROM "+quotedTable+" ORDER BY name")
	if err != nil {
		return compatibilityObservation{}, fmt.Errorf("select names: %w", err)
	}
	defer rows.Close()

	var resultRows []string
	for rows.Next() {
		var name string
		var visits int
		if err := rows.Scan(&name, &visits); err != nil {
			return compatibilityObservation{}, fmt.Errorf("scan row: %w", err)
		}
		resultRows = append(resultRows, fmt.Sprintf("%s:%d", name, visits))
	}
	if err := rows.Err(); err != nil {
		return compatibilityObservation{}, fmt.Errorf("read names: %w", err)
	}

	if _, err := db.ExecContext(ctx, "ALTER TABLE "+quotedTable+" CHANGE COLUMN visits visit_count INT NOT NULL DEFAULT 0"); err != nil {
		return compatibilityObservation{}, fmt.Errorf("alter change column: %w", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE "+quotedTable+" MODIFY COLUMN visit_count INT NOT NULL DEFAULT 0"); err != nil {
		return compatibilityObservation{}, fmt.Errorf("alter modify column: %w", err)
	}
	alteredColumnName, err := compatibilityColumnName(ctx, db, table, "visit_count")
	if err != nil {
		return compatibilityObservation{}, err
	}

	return compatibilityObservation{
		FirstInsertID:        firstInsertID,
		FirstRowsAffected:    firstRowsAffected,
		UpsertRowsAffected:   upsertRowsAffected,
		UpsertLastInsertID:   upsertLastInsertID,
		InsertIgnoreAffected: insertIgnoreAffected,
		InsertIgnoreLastID:   insertIgnoreLastID,
		ReplaceRowsAffected:  replaceRowsAffected,
		ReplaceLastInsertID:  replaceLastInsertID,
		Rows:                 resultRows,
		RollbackCount:        rollbackCount,
		DuplicateCode:        duplicateCode,
		DuplicateSQLState:    duplicateSQLState,
		DuplicateMentionsKey: duplicateMentionsKey,
		DataTooLongCode:      dataTooLongCode,
		DataTooLongSQLState:  dataTooLongSQLState,
		DataTooLongMessage:   dataTooLongMessage,
		ShowIDKey:            showIDKey,
		ShowNameKey:          showNameKey,
		ShowCreateHasEngine:  showCreateHasEngine,
		ShowCreateHasCharset: showCreateHasCharset,
		ShowNamePrefixSub:    showNamePrefixSub,
		DateFormatValue:      dateFormatValue,
		JSONExtractValue:     jsonExtractValue,
		ForeignKeyChecksTx:   foreignKeyChecksTx,
		ForeignKeyInsertOK:   foreignKeyInsertOK,
		AlteredColumnName:    alteredColumnName,
	}, nil
}

func compatibilityShowFieldKeys(ctx context.Context, db *sql.DB, quotedTable string) (idKey, nameKey string, err error) {
	rows, err := db.QueryContext(ctx, "SHOW FULL FIELDS FROM "+quotedTable)
	if err != nil {
		return "", "", fmt.Errorf("show full fields: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var field string
		var typ string
		var collation sql.NullString
		var nullValue string
		var key string
		var defaultValue sql.NullString
		var extra string
		var privileges string
		var comment string
		if err := rows.Scan(&field, &typ, &collation, &nullValue, &key, &defaultValue, &extra, &privileges, &comment); err != nil {
			return "", "", fmt.Errorf("scan show full fields: %w", err)
		}
		switch field {
		case "id":
			idKey = key
		case "name":
			nameKey = key
		}
	}
	if err := rows.Err(); err != nil {
		return "", "", fmt.Errorf("read show full fields: %w", err)
	}
	return idKey, nameKey, nil
}

func compatibilityShowCreateTableOptions(ctx context.Context, db *sql.DB, quotedTable string) (hasEngine, hasCharset bool, err error) {
	var tableName string
	var createSQL string
	if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+quotedTable).Scan(&tableName, &createSQL); err != nil {
		return false, false, fmt.Errorf("show create table: %w", err)
	}
	upper := strings.ToUpper(createSQL)
	return strings.Contains(upper, "ENGINE="), strings.Contains(upper, "CHARSET="), nil
}

func compatibilityShowKeySubPart(ctx context.Context, db *sql.DB, quotedTable, indexName string) (int, error) {
	rows, err := db.QueryContext(ctx, "SHOW KEYS FROM "+quotedTable)
	if err != nil {
		return 0, fmt.Errorf("show keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
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
		if err := rows.Scan(&table, &nonUnique, &keyName, &seqInIndex, &columnName, &collation, &cardinality, &subPart, &packed, &nullValue, &indexType, &comment, &indexComment, &visible, &expression); err != nil {
			return 0, fmt.Errorf("scan show keys: %w", err)
		}
		if keyName != indexName || !subPart.Valid {
			continue
		}
		value, err := strconv.Atoi(subPart.String)
		if err != nil {
			return 0, fmt.Errorf("parse show keys sub_part %q: %w", subPart.String, err)
		}
		return value, nil
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read show keys: %w", err)
	}
	return 0, fmt.Errorf("prefix index %s was not found in SHOW KEYS", indexName)
}

func runForeignKeyChecksTransactionScenario(ctx context.Context, db *sql.DB, table string) (string, bool, error) {
	parent := quoteMySQLCompatIdent(table + "_parents")
	child := quoteMySQLCompatIdent(table + "_children")
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+child)
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+parent)
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+parent+" (id INTEGER PRIMARY KEY AUTO_INCREMENT)"); err != nil {
		return "", false, fmt.Errorf("create fk parent table: %w", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE IF EXISTS "+child)
	defer db.ExecContext(ctx, "DROP TABLE IF EXISTS "+parent)
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+child+" (id INTEGER PRIMARY KEY AUTO_INCREMENT, parent_id INTEGER NOT NULL, FOREIGN KEY (parent_id) REFERENCES "+parent+"(id))"); err != nil {
		return "", false, fmt.Errorf("create fk child table: %w", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, fmt.Errorf("begin fk transaction: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		_ = tx.Rollback()
		return "", false, fmt.Errorf("disable foreign_key_checks in transaction: %w", err)
	}
	var checks string
	if err := tx.QueryRowContext(ctx, "SELECT @@FOREIGN_KEY_CHECKS").Scan(&checks); err != nil {
		_ = tx.Rollback()
		return "", false, fmt.Errorf("select transaction foreign_key_checks: %w", err)
	}
	insertOK := true
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+child+" (parent_id) VALUES (?)", 999); err != nil {
		insertOK = false
	}
	if err := tx.Rollback(); err != nil {
		return "", false, fmt.Errorf("rollback fk transaction: %w", err)
	}
	return checks, insertOK, nil
}

func compatibilityColumnName(ctx context.Context, db *sql.DB, table, column string) (string, error) {
	var columnName string
	if err := db.QueryRowContext(ctx, `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?
  AND column_name = ?
`, table, column).Scan(&columnName); err != nil {
		return "", fmt.Errorf("select altered column metadata: %w", err)
	}
	return columnName, nil
}

func mysqlErrorCodeState(err error) (uint16, string) {
	var mysqlErr *mysqlDriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return 0, ""
	}
	return mysqlErr.Number, string(mysqlErr.SQLState[:])
}

func mysqlErrorCodeStateMessage(err error) (uint16, string, string) {
	var mysqlErr *mysqlDriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return 0, "", ""
	}
	return mysqlErr.Number, string(mysqlErr.SQLState[:]), mysqlErr.Message
}

func quoteMySQLCompatIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}
