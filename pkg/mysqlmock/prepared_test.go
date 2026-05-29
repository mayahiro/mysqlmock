package mysqlmock

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestCountPlaceholdersIgnoresQuotedTextAndComments(t *testing.T) {
	sql := `
SELECT '?', "?", ` + "`?`" + `, id
FROM users
WHERE id = ? -- ignored ?
  AND name = ?
  AND note = 'escaped \' ?'
  /* ignored ? */
`
	if got := countPlaceholders(sql); got != 2 {
		t.Fatalf("countPlaceholders() = %d, want 2", got)
	}
}

func TestPreparedStatementDecodeExecuteArgs(t *testing.T) {
	stmt := &preparedStatement{ID: 1, ParamCount: 4, LongData: map[int][]byte{}}

	payload := make([]byte, 0, 64)
	payload = appendUint32(payload, 1)
	payload = append(payload, 0x00)
	payload = binary.LittleEndian.AppendUint32(payload, 1)
	payload = append(payload, 0x08)
	payload = append(payload, 0x01)
	payload = append(payload,
		fieldTypeLongLong, 0x00,
		fieldTypeString, 0x00,
		fieldTypeString, 0x00,
		fieldTypeNull, 0x00,
	)
	payload = binary.LittleEndian.AppendUint64(payload, 42)
	payload = appendLenEncBytes(payload, []byte("hello"))
	payload = appendLenEncBytes(payload, []byte("bytes"))

	got, err := stmt.decodeExecuteArgs(payload)
	if err != nil {
		t.Fatalf("decodeExecuteArgs(): %v", err)
	}
	want := []any{int64(42), "hello", "bytes", nil}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decodeExecuteArgs() = %#v, want %#v", got, want)
	}
}

func TestDecodePreparedValueCoversAdditionalTypes(t *testing.T) {
	datePayload := []byte{0x04}
	datePayload = appendUint16(datePayload, 2026)
	datePayload = append(datePayload, 5, 23)

	tests := []struct {
		name  string
		input []byte
		typ   preparedParamType
		want  any
	}{
		{
			name:  "newdate",
			input: datePayload,
			typ:   preparedParamType{FieldType: fieldTypeNewDate},
			want:  time.Date(2026, 5, 23, 0, 0, 0, 0, time.Local),
		},
		{
			name:  "enum",
			input: appendLenEncBytes(nil, []byte("active")),
			typ:   preparedParamType{FieldType: fieldTypeEnum},
			want:  "active",
		},
		{
			name:  "set",
			input: appendLenEncBytes(nil, []byte("read,write")),
			typ:   preparedParamType{FieldType: fieldTypeSet},
			want:  "read,write",
		},
		{
			name:  "tinyblob",
			input: appendLenEncBytes(nil, []byte("tiny")),
			typ:   preparedParamType{FieldType: fieldTypeTinyBlob},
			want:  []byte("tiny"),
		},
		{
			name:  "mediumblob",
			input: appendLenEncBytes(nil, []byte("medium")),
			typ:   preparedParamType{FieldType: fieldTypeMedBlob},
			want:  []byte("medium"),
		},
		{
			name:  "longblob",
			input: appendLenEncBytes(nil, []byte("long")),
			typ:   preparedParamType{FieldType: fieldTypeLongBlob},
			want:  []byte("long"),
		},
		{
			name:  "geometry",
			input: appendLenEncBytes(nil, []byte{0x01, 0x02, 0x03}),
			typ:   preparedParamType{FieldType: fieldTypeGeometry},
			want:  []byte{0x01, 0x02, 0x03},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, n, err := decodePreparedValue(tt.input, tt.typ)
			if err != nil {
				t.Fatalf("decodePreparedValue(): %v", err)
			}
			if n != len(tt.input) {
				t.Fatalf("decodePreparedValue() consumed %d bytes, want %d", n, len(tt.input))
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("decodePreparedValue() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestBinaryRowEncodesPreparedResultValues(t *testing.T) {
	got, err := binaryRow(
		[]resultColumn{
			{Name: "id", Type: fieldTypeLongLong},
			{Name: "name", Type: fieldTypeVarString},
			{Name: "data", Type: fieldTypeBlob},
			{Name: "missing", Type: fieldTypeVarString},
		},
		[]any{int64(42), "hello", []byte("bytes"), nil},
	)
	if err != nil {
		t.Fatalf("binaryRow(): %v", err)
	}

	want := []byte{0x00, 0x20}
	want = binary.LittleEndian.AppendUint64(want, 42)
	want = appendLenEncBytes(want, []byte("hello"))
	want = appendLenEncBytes(want, []byte("bytes"))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("binaryRow() = %#v, want %#v", got, want)
	}
}

func TestTranslateSQLConvertsMySQLTokensOutsideQuotedText(t *testing.T) {
	input := `
SELECT TRUE, FALSE, NOW(), CURRENT_TIMESTAMP(), 'TRUE FALSE NOW() AUTO_INCREMENT', ` + "`AUTO_INCREMENT`" + `
-- TRUE FALSE NOW() AUTO_INCREMENT
/* TRUE FALSE NOW() AUTO_INCREMENT */
`
	got := translateSQL(input)
	for _, want := range []string{
		"SELECT 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP",
		"'TRUE FALSE NOW() AUTO_INCREMENT'",
		"`AUTO_INCREMENT`",
		"-- TRUE FALSE NOW() AUTO_INCREMENT",
		"/* TRUE FALSE NOW() AUTO_INCREMENT */",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("translateSQL() = %q, want it to contain %q", got, want)
		}
	}
}

func TestTranslateSQLStripsCommonMySQLDDLOptions(t *testing.T) {
	input := `
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTO_INCREMENT COMMENT 'primary id',
  name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'display name',
  quantity INT UNSIGNED NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='users table';
`
	got := translateSQL(input)
	for _, want := range []string{
		"id INTEGER PRIMARY KEY AUTOINCREMENT",
		"name VARCHAR(255)",
		"quantity INT  NOT NULL DEFAULT 0",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("translateSQL() = %q, want it to contain %q", got, want)
		}
	}
	for _, unwanted := range []string{
		"AUTO_INCREMENT",
		"UNSIGNED",
		"CHARACTER SET",
		"COLLATE",
		"ENGINE",
		"COMMENT",
	} {
		if strings.Contains(strings.ToUpper(got), unwanted) {
			t.Fatalf("translateSQL() = %q, want it to omit %q", got, unwanted)
		}
	}
}

func TestTranslateSQLStripsAdditionalMySQLDDLOptions(t *testing.T) {
	input := `
CREATE TABLE events (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB AUTO_INCREMENT=100 ROW_FORMAT=DYNAMIC KEY_BLOCK_SIZE=8 STATS_PERSISTENT=1;
`
	got := translateSQL(input)
	for _, want := range []string{
		"id INTEGER PRIMARY KEY AUTOINCREMENT",
		"created_at DATETIME DEFAULT CURRENT_TIMESTAMP",
		"updated_at DATETIME DEFAULT CURRENT_TIMESTAMP",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("translateSQL() = %q, want it to contain %q", got, want)
		}
	}
	for _, unwanted := range []string{
		"CURRENT_TIMESTAMP(6)",
		"ON UPDATE",
		"AUTO_INCREMENT=100",
		"ROW_FORMAT",
		"KEY_BLOCK_SIZE",
		"STATS_PERSISTENT",
	} {
		if strings.Contains(strings.ToUpper(got), unwanted) {
			t.Fatalf("translateSQL() = %q, want it to omit %q", got, unwanted)
		}
	}
}

func TestTranslateSQLConvertsMySQLIndexDDL(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "create index using before on",
			input: "CREATE INDEX idx_users_email USING BTREE ON users (email);",
			want:  "CREATE INDEX " + quoteIdent(sqliteIndexName("users", "idx_users_email")) + " ON users (email)",
		},
		{
			name:  "create index using after columns",
			input: "CREATE INDEX idx_users_name ON users (name) USING BTREE;",
			want:  "CREATE INDEX " + quoteIdent(sqliteIndexName("users", "idx_users_name")) + " ON users (name)",
		},
		{
			name:  "create index prefix and invisible",
			input: "CREATE INDEX idx_users_name_prefix ON users (name(10)) INVISIBLE;",
			want:  "CREATE INDEX " + quoteIdent(sqliteIndexName("users", "idx_users_name_prefix")) + " ON users (name)",
		},
		{
			name:  "alter table add index",
			input: "ALTER TABLE users ADD INDEX idx_users_name (name) USING BTREE;",
			want:  "CREATE INDEX " + quoteIdent(sqliteIndexName("users", "idx_users_name")) + " ON users (name)",
		},
		{
			name:  "alter table add unique key",
			input: "ALTER TABLE `users` ADD UNIQUE KEY `idx_users_email` (`email`);",
			want:  "CREATE UNIQUE INDEX " + quoteIdent(sqliteIndexName("users", "idx_users_email")) + " ON `users` (`email`)",
		},
		{
			name:  "concat function",
			input: "SELECT CONCAT(first_name, ' ', last_name) FROM users",
			want:  "SELECT (first_name) || (' ') || (last_name) FROM users",
		},
		{
			name:  "date format function",
			input: "SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') FROM users",
			want:  "SELECT strftime('%Y-%m-%d %H:%M:%S', created_at) FROM users",
		},
		{
			name:  "json extract function",
			input: "SELECT JSON_EXTRACT(payload, '$.role') FROM users",
			want:  "SELECT json_quote(json_extract(payload, '$.role')) FROM users",
		},
		{
			name:  "json unquote extract function",
			input: "SELECT JSON_UNQUOTE(JSON_EXTRACT(payload, '$.role')) FROM users",
			want:  "SELECT json_extract(payload, '$.role') FROM users",
		},
		{
			name:  "cast signed function",
			input: "SELECT CAST(score AS SIGNED) FROM users",
			want:  "SELECT CAST(score AS INTEGER) FROM users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := translateSQL(tt.input); got != tt.want {
				t.Fatalf("translateSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTranslateSQLStatementsConvertsTiDBCreateTable(t *testing.T) {
	input := `
CREATE TABLE update_patterns (
  id BIGINT PRIMARY KEY /*T![clustered_index] CLUSTERED */ AUTO_RANDOM(5),
  code VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  channel_id BIGINT NOT NULL,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uniq_update_patterns_code (code),
  KEY idx_update_patterns_channel_id (channel_id(20))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_RANDOM_BASE=100;
`
	got := translateSQLStatements(input)
	if len(got) != 3 {
		t.Fatalf("translateSQLStatements() returned %d statements, want 3: %#v", len(got), got)
	}
	for _, want := range []string{
		"id INTEGER PRIMARY KEY AUTOINCREMENT",
		"code VARCHAR(255)",
		"updated_at DATETIME DEFAULT CURRENT_TIMESTAMP",
	} {
		if !strings.Contains(got[0], want) {
			t.Fatalf("create table translation = %q, want it to contain %q", got[0], want)
		}
	}
	for _, unwanted := range []string{
		"AUTO_RANDOM",
		"AUTO_RANDOM_BASE",
		"CLUSTERED",
		"ON UPDATE",
		"CHARACTER SET",
		"COLLATE",
	} {
		if strings.Contains(strings.ToUpper(got[0]), unwanted) {
			t.Fatalf("create table translation = %q, want it to omit %q", got[0], unwanted)
		}
	}
	wantUniqueIndex := "CREATE UNIQUE INDEX " + quoteIdent(sqliteIndexName("update_patterns", "uniq_update_patterns_code")) + " ON update_patterns (code)"
	if got[1] != wantUniqueIndex {
		t.Fatalf("unique index translation = %q", got[1])
	}
	wantIndex := "CREATE INDEX " + quoteIdent(sqliteIndexName("update_patterns", "idx_update_patterns_channel_id")) + " ON update_patterns (channel_id)"
	if got[2] != wantIndex {
		t.Fatalf("index translation = %q", got[2])
	}
}

func TestTranslateSQLStatementsConvertsMySQLDumpAutoIncrementPrimaryKey(t *testing.T) {
	input := `
CREATE TABLE users (
  id bigint NOT NULL AUTO_INCREMENT,
  email varchar(255) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_users_email (email)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4;
`
	got := translateSQLStatements(input)
	if len(got) != 2 {
		t.Fatalf("translateSQLStatements() returned %d statements, want 2: %#v", len(got), got)
	}
	if !strings.Contains(got[0], "id INTEGER PRIMARY KEY AUTOINCREMENT") {
		t.Fatalf("create table translation = %q, want auto-increment primary key column", got[0])
	}
	if strings.Contains(got[0], "PRIMARY KEY (id)") || strings.Contains(got[0], "AUTO_INCREMENT") {
		t.Fatalf("create table translation = %q, want table primary key and AUTO_INCREMENT removed", got[0])
	}
	wantUniqueIndex := "CREATE UNIQUE INDEX " + quoteIdent(sqliteIndexName("users", "uniq_users_email")) + " ON users (email)"
	if got[1] != wantUniqueIndex {
		t.Fatalf("unique index translation = %q, want %q", got[1], wantUniqueIndex)
	}
}

func TestTranslateSQLStatementsConvertsInlineAutoIncrementPrimaryKey(t *testing.T) {
	input := `
CREATE TABLE users (
  id bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,
  email varchar(255) NOT NULL
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4;
`
	got := translateSQLStatements(input)
	if len(got) != 1 {
		t.Fatalf("translateSQLStatements() returned %d statements, want 1: %#v", len(got), got)
	}
	if !strings.Contains(got[0], "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT") {
		t.Fatalf("create table translation = %q, want inline auto-increment primary key column", got[0])
	}
	if strings.Contains(got[0], "AUTO_INCREMENT") {
		t.Fatalf("create table translation = %q, want AUTO_INCREMENT removed", got[0])
	}
}

func TestTranslateSQLStatementsConvertsCompositePrimaryKey(t *testing.T) {
	input := `
CREATE TABLE links (
  tenant_id bigint unsigned NOT NULL,
  id bigint NOT NULL AUTO_INCREMENT,
  code varchar(255) NOT NULL,
  locale varchar(16) NOT NULL,
  PRIMARY KEY USING BTREE (id, tenant_id, code(191), locale)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`
	got := translateSQLStatements(input)
	if len(got) != 1 {
		t.Fatalf("translateSQLStatements() returned %d statements, want 1: %#v", len(got), got)
	}
	for _, want := range []string{
		"tenant_id bigint  NOT NULL",
		"id bigint NOT NULL",
		"code varchar(255) NOT NULL",
		"PRIMARY KEY (id, tenant_id, code, locale)",
	} {
		if !strings.Contains(got[0], want) {
			t.Fatalf("create table translation = %q, want it to contain %q", got[0], want)
		}
	}
	for _, unwanted := range []string{
		"AUTO_INCREMENT",
		"USING BTREE",
		"code(191)",
		"UNSIGNED",
	} {
		if strings.Contains(strings.ToUpper(got[0]), strings.ToUpper(unwanted)) {
			t.Fatalf("create table translation = %q, want it to omit %q", got[0], unwanted)
		}
	}
}

func TestTranslateSQLStripsLockingClause(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "for update nowait",
			input: "SELECT * FROM jobs WHERE status = 'queued' ORDER BY id LIMIT 1 FOR UPDATE NOWAIT;",
			want:  "SELECT * FROM jobs WHERE status = 'queued' ORDER BY id LIMIT 1;",
		},
		{
			name:  "for update skip locked",
			input: "SELECT * FROM jobs FOR UPDATE SKIP LOCKED",
			want:  "SELECT * FROM jobs",
		},
		{
			name:  "lock in share mode",
			input: "SELECT * FROM jobs LOCK IN SHARE MODE",
			want:  "SELECT * FROM jobs",
		},
		{
			name:  "quoted text is untouched",
			input: "SELECT 'FOR UPDATE NOWAIT'",
			want:  "SELECT 'FOR UPDATE NOWAIT'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := translateSQL(tt.input); got != tt.want {
				t.Fatalf("translateSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSchemaStatementsFromDumpKeepsSchemaDDL(t *testing.T) {
	input := `
-- MySQL dump
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
CREATE DATABASE ` + "`ignored`" + ` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;
USE ` + "`ignored`" + `;
DROP TABLE IF EXISTS ` + "`users`" + `;
CREATE TABLE ` + "`users`" + ` (
  ` + "`id`" + ` BIGINT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM(5) */,
  ` + "`email`" + ` varchar(255) NOT NULL,
  UNIQUE KEY ` + "`uniq_users_email`" + ` (` + "`email`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
LOCK TABLES ` + "`users`" + ` WRITE;
INSERT INTO ` + "`users`" + ` VALUES (1, 'alice@example.com');
UNLOCK TABLES;
ALTER TABLE ` + "`users`" + ` ADD KEY ` + "`idx_users_email`" + ` (` + "`email`" + `);
`
	got := schemaStatementsFromDump(input)
	want := []string{
		"DROP TABLE IF EXISTS `users`",
		"CREATE TABLE `users` (\n  `id` BIGINT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM(5) */,\n  `email` varchar(255) NOT NULL,\n  UNIQUE KEY `uniq_users_email` (`email`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		"ALTER TABLE `users` ADD KEY `idx_users_email` (`email`)",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("schemaStatementsFromDump() = %#v, want %#v", got, want)
	}
}

func TestParseMySQLColumnMetadataZerofill(t *testing.T) {
	metadata := parseMySQLColumnMetadata(`
CREATE TABLE partitioned_zerofill_values (
  id INTEGER NOT NULL AUTO_INCREMENT,
  zero_padding INT(5) ZEROFILL NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY HASH(id) PARTITIONS 4;
`)
	if len(metadata) != 2 {
		t.Fatalf("metadata length = %d, want 2: %#v", len(metadata), metadata)
	}
	if metadata[0].TableName != "partitioned_zerofill_values" || metadata[0].ColumnName != "id" || !metadata[0].AutoIncrement {
		t.Fatalf("metadata = %#v, want id auto increment", metadata[0])
	}
	if metadata[1].TableName != "partitioned_zerofill_values" || metadata[1].ColumnName != "zero_padding" || metadata[1].ZeroFillWidth != 5 {
		t.Fatalf("metadata = %#v, want zero_padding width 5", metadata[1])
	}

	tableName, ok := parseSimpleSelectTableName("SELECT zero_padding FROM partitioned_zerofill_values WHERE id = ?")
	if !ok || tableName != "partitioned_zerofill_values" {
		t.Fatalf("parse simple select table = %q/%v, want partitioned_zerofill_values/true", tableName, ok)
	}
	if !isSelectAllItem("`partitioned_zerofill_values`.*") {
		t.Fatal("qualified select-all item was not recognized")
	}
	columnName, ok := selectItemColumnName("`partitioned_zerofill_values`.`zero_padding` AS `zp`")
	if !ok || columnName != "zero_padding" {
		t.Fatalf("select item column = %q/%v, want zero_padding/true", columnName, ok)
	}
	if columnName, ok := selectItemColumnName("COUNT(*) AS `zero_padding`"); ok {
		t.Fatalf("select item expression column = %q/%v, want unsupported expression", columnName, ok)
	}
}

func TestPreparedStatementsWithGoSQLDriverOverPipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server, err := New(WithConfig(preparedTestConfig()))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.openBackend(ctx); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	network := fmt.Sprintf("mysqlmock_pipe_%d", time.Now().UnixNano())
	mysql.RegisterDialContext(network, func(ctx context.Context, addr string) (net.Conn, error) {
		clientConn, serverConn := net.Pipe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.handleNetConn(serverConn)
		}()
		return clientConn, nil
	})

	db, err := sql.Open("mysql", fmt.Sprintf("user:password@%s(mysqlmock)/mysqlmock?charset=utf8mb4&parseTime=true", network))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
		wg.Wait()
		if err := server.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	}()
	db.SetMaxOpenConns(1)

	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE id = ?", 1).Scan(&name); err != nil {
		t.Fatalf("direct prepared query: %v", err)
	}
	if name != "Alice" {
		t.Fatalf("unexpected direct prepared query name: %s", name)
	}

	stmt, err := db.PrepareContext(ctx, "SELECT name FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("prepare select: %v", err)
	}
	defer stmt.Close()
	if err := stmt.QueryRowContext(ctx, 2).Scan(&name); err != nil {
		t.Fatalf("prepared query: %v", err)
	}
	if name != "Bob" {
		t.Fatalf("unexpected prepared query name: %s", name)
	}

	result, err := db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Carol", "carol@example.com")
	if err != nil {
		t.Fatalf("direct prepared exec: %v", err)
	}
	insertID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if insertID == 0 {
		t.Fatal("expected non-zero insert id")
	}

	var gotInt int64
	var gotText string
	var gotBytes []byte
	var gotNull sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT ?, ?, ?, ?", int64(42), "hello", []byte("bytes"), nil).Scan(&gotInt, &gotText, &gotBytes, &gotNull); err != nil {
		t.Fatalf("scalar round trip: %v", err)
	}
	if gotInt != 42 || gotText != "hello" || string(gotBytes) != "bytes" || gotNull.Valid {
		t.Fatalf("unexpected scalar round trip: int=%d text=%q bytes=%q null=%v", gotInt, gotText, gotBytes, gotNull.Valid)
	}

	inputTime := time.Date(2026, 5, 23, 14, 15, 16, 123456000, time.UTC)
	var gotBool bool
	var gotUint uint64
	var gotFloat float64
	var gotTime string
	if err := db.QueryRowContext(ctx, "SELECT ?, ?, ?, ?", true, uint64(99), float64(3.5), inputTime).Scan(&gotBool, &gotUint, &gotFloat, &gotTime); err != nil {
		t.Fatalf("additional scalar round trip: %v", err)
	}
	if !gotBool || gotUint != 99 || gotFloat != 3.5 || gotTime != "2026-05-23 14:15:16.123456" {
		t.Fatalf("unexpected additional scalar round trip: bool=%v uint=%d float=%f time=%q", gotBool, gotUint, gotFloat, gotTime)
	}
}

func preparedTestConfig() Config {
	cfg := DefaultConfig()
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
