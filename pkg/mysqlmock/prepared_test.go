package mysqlmock

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
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
