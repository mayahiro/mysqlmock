package mysqlmock

import (
	"io"
	"net"
	"testing"
	"time"
)

var (
	benchmarkProtocolBytes  int
	benchmarkProtocolWrites int
)

func BenchmarkProtocolWritePacket(b *testing.B) {
	payload := make([]byte, 128)
	conn := &mysqlConn{netConn: &benchmarkDiscardConn{}}
	if err := conn.writePacket(1, payload); err != nil {
		b.Fatalf("warm writePacket: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := conn.writePacket(byte(i), payload); err != nil {
			b.Fatalf("writePacket: %v", err)
		}
	}
	b.StopTimer()

	discard := conn.netConn.(*benchmarkDiscardConn)
	benchmarkProtocolBytes = discard.bytes
	benchmarkProtocolWrites = discard.writes
}

func BenchmarkProtocolWriteResultSet(b *testing.B) {
	for _, rowCount := range []int{10, 1000} {
		b.Run("rows_"+stringForCacheTestInt(rowCount), func(b *testing.B) {
			rs := benchmarkProtocolResultSet(rowCount)
			discard := &benchmarkDiscardConn{}
			conn := &mysqlConn{
				netConn:   discard,
				server:    &Server{},
				currentDB: "mysqlmock",
			}
			if err := conn.writeResultSet(1, rs); err != nil {
				b.Fatalf("warm writeResultSet: %v", err)
			}
			discard.reset()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := conn.writeResultSet(1, rs); err != nil {
					b.Fatalf("writeResultSet: %v", err)
				}
			}
			b.StopTimer()

			benchmarkProtocolBytes = discard.bytes
			benchmarkProtocolWrites = discard.writes
		})
	}
}

func benchmarkProtocolResultSet(rowCount int) resultSet {
	columns := []resultColumn{
		{Name: "id", Type: fieldTypeLongLong},
		{Name: "name", Type: fieldTypeVarString},
		{Name: "group_id", Type: fieldTypeLongLong},
		{Name: "score", Type: fieldTypeLongLong},
		{Name: "email", Type: fieldTypeVarString},
		{Name: "active", Type: fieldTypeLongLong},
		{Name: "created_at", Type: fieldTypeVarString},
		{Name: "amount", Type: fieldTypeDouble},
	}
	rows := make([][]any, rowCount)
	for i := 0; i < rowCount; i++ {
		value := stringForCacheTestInt(i + 1)
		rows[i] = []any{
			int64(i + 1),
			"name-" + value,
			int64(i % 100),
			int64((i + 1) * 2),
			"user-" + value + "@example.com",
			int64(i % 2),
			"2024-01-01 00:00:00",
			float64(i+1) / 10,
		}
	}
	return resultSet{Columns: columns, Rows: rows}
}

type benchmarkDiscardConn struct {
	bytes  int
	writes int
}

func (c *benchmarkDiscardConn) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *benchmarkDiscardConn) Write(p []byte) (int, error) {
	c.bytes += len(p)
	c.writes++
	return len(p), nil
}

func (c *benchmarkDiscardConn) Close() error {
	return nil
}

func (c *benchmarkDiscardConn) LocalAddr() net.Addr {
	return benchmarkAddr("local")
}

func (c *benchmarkDiscardConn) RemoteAddr() net.Addr {
	return benchmarkAddr("remote")
}

func (c *benchmarkDiscardConn) SetDeadline(time.Time) error {
	return nil
}

func (c *benchmarkDiscardConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *benchmarkDiscardConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *benchmarkDiscardConn) reset() {
	c.bytes = 0
	c.writes = 0
}

type benchmarkAddr string

func (a benchmarkAddr) Network() string {
	return string(a)
}

func (a benchmarkAddr) String() string {
	return string(a)
}
