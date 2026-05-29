package mysqlmock

import (
	"context"
	"database/sql"
	"net"
	"reflect"
	"testing"
)

var (
	benchmarkSQLiteResultRows    int
	benchmarkSQLiteResultColumns int
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

func BenchmarkSQLiteResultStreaming(b *testing.B) {
	ctx := context.Background()
	for _, rowCount := range []int{10, 1000, 100000} {
		for _, columnCount := range []int{2, 8} {
			name := "rows_" + stringForCacheTestInt(rowCount) + "/cols_" + stringForCacheTestInt(columnCount)
			b.Run(name, func(b *testing.B) {
				conn, cleanup := newSQLiteResultBenchmarkConn(b, ctx, rowCount)
				defer cleanup()

				query := benchmarkSQLiteResultQuery(columnCount)
				b.Run("streaming", func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						resp, err := conn.querySQLiteText(ctx, query, true)
						if err != nil {
							b.Fatalf("query SQLite text stream: %v", err)
						}
						stream, ok := resp.(*sqliteResultSet)
						if !ok {
							b.Fatalf("query SQLite text stream returned %T, want *sqliteResultSet", resp)
						}
						rows, err := consumeBenchmarkSQLiteResultStream(stream)
						if err != nil {
							b.Fatalf("consume SQLite stream: %v", err)
						}
						if rows != rowCount {
							b.Fatalf("streamed row count = %d, want %d", rows, rowCount)
						}
						benchmarkSQLiteResultRows = rows
						benchmarkSQLiteResultColumns = len(stream.Columns)
					}
				})

				b.Run("materialized", func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						resp, err := conn.querySQLiteText(ctx, query, false)
						if err != nil {
							b.Fatalf("query SQLite text materialized: %v", err)
						}
						rs, ok := resp.(resultSet)
						if !ok {
							b.Fatalf("query SQLite text materialized returned %T, want resultSet", resp)
						}
						if len(rs.Rows) != rowCount {
							b.Fatalf("materialized row count = %d, want %d", len(rs.Rows), rowCount)
						}
						benchmarkSQLiteResultRows = len(rs.Rows)
						benchmarkSQLiteResultColumns = len(rs.Columns)
					}
				})
			})
		}
	}
}

func newSQLiteResultBenchmarkConn(b *testing.B, ctx context.Context, rowCount int) (*mysqlConn, func()) {
	b.Helper()

	server, err := New(WithConfig(DefaultConfig()))
	if err != nil {
		b.Fatalf("new server: %v", err)
	}
	if err := server.openBackend(ctx); err != nil {
		b.Fatalf("open backend: %v", err)
	}
	sqliteConn, err := server.db.Conn(ctx)
	if err != nil {
		_ = server.closeBackend()
		b.Fatalf("sqlite conn: %v", err)
	}
	if err := setupSQLiteResultBenchmarkRows(ctx, sqliteConn, rowCount); err != nil {
		_ = sqliteConn.Close()
		_ = server.closeBackend()
		b.Fatalf("setup rows: %v", err)
	}

	conn := &mysqlConn{
		sqliteConn: sqliteConn,
		server:     server,
		currentDB:  "mysqlmock",
	}
	cleanup := func() {
		_ = sqliteConn.Close()
		_ = server.closeBackend()
	}
	return conn, cleanup
}

func setupSQLiteResultBenchmarkRows(ctx context.Context, conn *sql.Conn, rowCount int) error {
	if _, err := conn.ExecContext(ctx, `
CREATE TABLE bench_sqlite_result (
  id INTEGER,
  name TEXT,
  group_id INTEGER,
  score INTEGER,
  email TEXT,
  active INTEGER,
  created_at TEXT,
  amount REAL
)`); err != nil {
		return err
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO bench_sqlite_result (
  id,
  name,
  group_id,
  score,
  email,
  active,
  created_at,
  amount
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for i := 1; i <= rowCount; i++ {
		value := stringForCacheTestInt(i)
		if _, err := stmt.ExecContext(
			ctx,
			i,
			"name-"+value,
			i%100,
			i*2,
			"user-"+value+"@example.com",
			i%2,
			"2024-01-01 00:00:00",
			float64(i)/10,
		); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return err
		}
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func benchmarkSQLiteResultQuery(columnCount int) string {
	if columnCount == 2 {
		return "SELECT id, name FROM bench_sqlite_result"
	}
	return "SELECT id, name, group_id, score, email, active, created_at, amount FROM bench_sqlite_result"
}

func consumeBenchmarkSQLiteResultStream(rs *sqliteResultSet) (int, error) {
	defer rs.Close()

	count := 0
	for {
		_, ok, err := rs.nextRow(false)
		if err != nil {
			return 0, err
		}
		if !ok {
			return count, nil
		}
		count++
	}
}
