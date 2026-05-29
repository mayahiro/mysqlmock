package mysqlmock

import (
	"context"
	"net"
	"reflect"
	"testing"
)

func TestExecuteQueryCOMQueryUsesStreamingSQLiteResultSet(t *testing.T) {
	ctx := context.Background()
	conn, cleanup := newSQLiteResultTestConn(t, ctx)
	defer cleanup()

	resp, err := conn.executeQuery(ctx, "COM_QUERY", "SELECT id, name FROM stream_users ORDER BY id")
	if err != nil {
		t.Fatalf("executeQuery(): %v", err)
	}
	stream, ok := resp.(*sqliteResultSet)
	if !ok {
		t.Fatalf("executeQuery() returned %T, want *sqliteResultSet", resp)
	}

	rs, err := stream.materialize()
	if err != nil {
		t.Fatalf("materialize streaming result: %v", err)
	}
	got := resultRows(rs)
	want := [][]any{{int64(1), "Alice"}, {int64(2), "Bob"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("streaming rows = %#v, want %#v", got, want)
	}
}

func TestExecuteQueryPreparedPathKeepsMaterializedSQLiteResultSet(t *testing.T) {
	ctx := context.Background()
	conn, cleanup := newSQLiteResultTestConn(t, ctx)
	defer cleanup()

	resp, err := conn.executeQuery(ctx, "COM_STMT_EXECUTE", "SELECT id, name FROM stream_users ORDER BY id")
	if err != nil {
		t.Fatalf("executeQuery(): %v", err)
	}
	rs, ok := resp.(resultSet)
	if !ok {
		t.Fatalf("executeQuery() returned %T, want resultSet", resp)
	}
	got := resultRows(rs)
	want := [][]any{{int64(1), "Alice"}, {int64(2), "Bob"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("materialized rows = %#v, want %#v", got, want)
	}
}

func TestWriteSQLiteTextResultSetStreamsRows(t *testing.T) {
	ctx := context.Background()
	conn, cleanup := newSQLiteResultTestConn(t, ctx)
	defer cleanup()

	resp, err := conn.executeQuery(ctx, "COM_QUERY", "SELECT id, name FROM stream_users ORDER BY id")
	if err != nil {
		t.Fatalf("executeQuery(): %v", err)
	}
	stream, ok := resp.(*sqliteResultSet)
	if !ok {
		t.Fatalf("executeQuery() returned %T, want *sqliteResultSet", resp)
	}

	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	conn.netConn = serverSide
	errCh := make(chan error, 1)
	go func() {
		defer serverSide.Close()
		errCh <- conn.writeSQLiteTextResultSet(1, stream)
	}()

	reader := &mysqlConn{netConn: clientSide}
	payload, seq, err := reader.readPacket()
	if err != nil {
		t.Fatalf("read column count packet: %v", err)
	}
	if seq != 1 || len(payload) != 1 || payload[0] != 2 {
		t.Fatalf("column count packet seq=%d payload=%#v, want seq=1 count=2", seq, payload)
	}
	for wantSeq := byte(2); wantSeq <= 4; wantSeq++ {
		if _, seq, err := reader.readPacket(); err != nil {
			t.Fatalf("read metadata packet seq=%d: %v", wantSeq, err)
		} else if seq != wantSeq {
			t.Fatalf("metadata packet seq = %d, want %d", seq, wantSeq)
		}
	}

	firstRow, seq, err := reader.readPacket()
	if err != nil {
		t.Fatalf("read first row: %v", err)
	}
	if seq != 5 || !reflect.DeepEqual(firstRow, []byte{1, '1', 5, 'A', 'l', 'i', 'c', 'e'}) {
		t.Fatalf("first row seq=%d payload=%#v", seq, firstRow)
	}
	secondRow, seq, err := reader.readPacket()
	if err != nil {
		t.Fatalf("read second row: %v", err)
	}
	if seq != 6 || !reflect.DeepEqual(secondRow, []byte{1, '2', 3, 'B', 'o', 'b'}) {
		t.Fatalf("second row seq=%d payload=%#v", seq, secondRow)
	}
	if _, seq, err := reader.readPacket(); err != nil {
		t.Fatalf("read final EOF: %v", err)
	} else if seq != 7 {
		t.Fatalf("final EOF seq = %d, want 7", seq)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeSQLiteTextResultSet(): %v", err)
	}
}

func TestWriteSQLiteTextResultSetUsesDeprecatedEOFCapability(t *testing.T) {
	ctx := context.Background()
	conn, cleanup := newSQLiteResultTestConn(t, ctx)
	defer cleanup()
	conn.clientCaps = clientProtocol41 | clientTransactions | clientDeprecateEOF

	resp, err := conn.executeQuery(ctx, "COM_QUERY", "SELECT id, name FROM stream_users ORDER BY id")
	if err != nil {
		t.Fatalf("executeQuery(): %v", err)
	}
	stream, ok := resp.(*sqliteResultSet)
	if !ok {
		t.Fatalf("executeQuery() returned %T, want *sqliteResultSet", resp)
	}

	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	conn.netConn = serverSide
	errCh := make(chan error, 1)
	go func() {
		defer serverSide.Close()
		errCh <- conn.writeSQLiteTextResultSet(1, stream)
	}()

	reader := &mysqlConn{netConn: clientSide}
	if payload, seq, err := reader.readPacket(); err != nil {
		t.Fatalf("read column count packet: %v", err)
	} else if seq != 1 || len(payload) != 1 || payload[0] != 2 {
		t.Fatalf("column count packet seq=%d payload=%#v, want seq=1 count=2", seq, payload)
	}
	for wantSeq := byte(2); wantSeq <= 3; wantSeq++ {
		if _, seq, err := reader.readPacket(); err != nil {
			t.Fatalf("read metadata packet seq=%d: %v", wantSeq, err)
		} else if seq != wantSeq {
			t.Fatalf("metadata packet seq = %d, want %d", seq, wantSeq)
		}
	}

	firstRow, seq, err := reader.readPacket()
	if err != nil {
		t.Fatalf("read first row: %v", err)
	}
	if seq != 4 || !reflect.DeepEqual(firstRow, []byte{1, '1', 5, 'A', 'l', 'i', 'c', 'e'}) {
		t.Fatalf("first row seq=%d payload=%#v", seq, firstRow)
	}
	secondRow, seq, err := reader.readPacket()
	if err != nil {
		t.Fatalf("read second row: %v", err)
	}
	if seq != 5 || !reflect.DeepEqual(secondRow, []byte{1, '2', 3, 'B', 'o', 'b'}) {
		t.Fatalf("second row seq=%d payload=%#v", seq, secondRow)
	}
	finalOK, seq, err := reader.readPacket()
	if err != nil {
		t.Fatalf("read final OK terminator: %v", err)
	}
	if seq != 6 || !reflect.DeepEqual(finalOK, []byte{0xfe, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}) {
		t.Fatalf("final terminator seq=%d payload=%#v", seq, finalOK)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeSQLiteTextResultSet(): %v", err)
	}
}

func newSQLiteResultTestConn(t *testing.T, ctx context.Context) (*mysqlConn, func()) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Schema = []string{`
CREATE TABLE stream_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL
);`}
	cfg.Seed = map[string][]map[string]any{
		"stream_users": {
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		},
	}
	server, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.openBackend(ctx); err != nil {
		t.Fatalf("open backend: %v", err)
	}
	sqliteConn, err := server.db.Conn(ctx)
	if err != nil {
		_ = server.closeBackend()
		t.Fatalf("sqlite conn: %v", err)
	}
	if _, err := sqliteConn.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		_ = sqliteConn.Close()
		_ = server.closeBackend()
		t.Fatalf("sqlite conn init: %v", err)
	}

	conn := &mysqlConn{
		sqliteConn:  sqliteConn,
		server:      server,
		statusFlags: serverStatusAutocommit,
		currentDB:   "mysqlmock",
	}
	cleanup := func() {
		_ = sqliteConn.Close()
		_ = server.closeBackend()
	}
	return conn, cleanup
}

func resultRows(rs resultSet) [][]any {
	rows := make([][]any, len(rs.Rows))
	for i, row := range rs.Rows {
		rows[i] = make([]any, len(row))
		for j, value := range row {
			switch v := value.(type) {
			case int:
				rows[i][j] = int64(v)
			case int32:
				rows[i][j] = int64(v)
			case int64:
				rows[i][j] = v
			case []byte:
				rows[i][j] = string(v)
			default:
				rows[i][j] = v
			}
		}
	}
	return rows
}
