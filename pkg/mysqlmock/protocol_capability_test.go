package mysqlmock

import (
	"errors"
	"io"
	"net"
	"testing"
)

func TestServerCapabilityFlagsAdvertiseDeprecatedEOF(t *testing.T) {
	caps := serverCapabilityFlags()
	if caps&clientDeprecateEOF == 0 {
		t.Fatal("server capabilities did not advertise CLIENT_DEPRECATE_EOF")
	}
}

func TestStmtPrepareSkipsParameterEOFWhenDeprecatedEOFIsNegotiated(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()

	conn := &mysqlConn{
		netConn:         serverSide,
		server:          &Server{},
		currentDB:       "mysqlmock",
		clientCaps:      clientProtocol41 | clientTransactions | clientDeprecateEOF,
		nextStatementID: 1,
		statements:      map[uint32]*preparedStatement{},
	}
	errCh := make(chan error, 1)
	go func() {
		defer serverSide.Close()
		errCh <- conn.handleStmtPrepare("SELECT ?")
	}()

	reader := &mysqlConn{netConn: clientSide}
	if _, seq, err := reader.readPacket(); err != nil {
		t.Fatalf("read COM_STMT_PREPARE_OK: %v", err)
	} else if seq != 1 {
		t.Fatalf("COM_STMT_PREPARE_OK seq = %d, want 1", seq)
	}
	if _, seq, err := reader.readPacket(); err != nil {
		t.Fatalf("read parameter definition: %v", err)
	} else if seq != 2 {
		t.Fatalf("parameter definition seq = %d, want 2", seq)
	}
	if payload, seq, err := reader.readPacket(); err == nil {
		t.Fatalf("unexpected parameter metadata terminator seq=%d payload=%#v", seq, payload)
	} else if !errors.Is(err, io.EOF) {
		t.Fatalf("read after parameter definition returned %v, want EOF", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("handleStmtPrepare(): %v", err)
	}
}
