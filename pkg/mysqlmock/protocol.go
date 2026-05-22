package mysqlmock

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	clientLongPassword     uint32 = 0x00000001
	clientLongFlag         uint32 = 0x00000004
	clientConnectWithDB    uint32 = 0x00000008
	clientProtocol41       uint32 = 0x00000200
	clientTransactions     uint32 = 0x00002000
	clientSecureConnection uint32 = 0x00008000
	clientPluginAuth       uint32 = 0x00080000

	serverStatusInTrans    uint16 = 0x0001
	serverStatusAutocommit uint16 = 0x0002

	commandQuit         byte = 0x01
	commandInitDB       byte = 0x02
	commandQuery        byte = 0x03
	commandPing         byte = 0x0e
	commandStmtPrepare  byte = 0x16
	commandStmtExecute  byte = 0x17
	commandStmtSendLong byte = 0x18
	commandStmtClose    byte = 0x19
	commandStmtReset    byte = 0x1a
	commandResetConn    byte = 0x1f

	fieldTypeDecimal   byte = 0x00
	fieldTypeTiny      byte = 0x01
	fieldTypeShort     byte = 0x02
	fieldTypeLong      byte = 0x03
	fieldTypeFloat     byte = 0x04
	fieldTypeDouble    byte = 0x05
	fieldTypeNull      byte = 0x06
	fieldTypeTimestamp byte = 0x07
	fieldTypeLongLong  byte = 0x08
	fieldTypeInt24     byte = 0x09
	fieldTypeDate      byte = 0x0a
	fieldTypeTime      byte = 0x0b
	fieldTypeDateTime  byte = 0x0c
	fieldTypeYear      byte = 0x0d
	fieldTypeVarChar   byte = 0x0f
	fieldTypeBit       byte = 0x10
	fieldTypeJSON      byte = 0xf5
	fieldTypeNewDec    byte = 0xf6
	fieldTypeVarString byte = 0xfd
	fieldTypeString    byte = 0xfe
	fieldTypeBlob      byte = 0xfc

	mysqlErrUnknown      uint16 = 1105
	mysqlErrNoSuchTable  uint16 = 1146
	mysqlErrDupEntry     uint16 = 1062
	mysqlErrBadNull      uint16 = 1048
	mysqlErrNoReferenced uint16 = 1452
	mysqlErrCheck        uint16 = 3819
)

type mysqlConn struct {
	netConn      net.Conn
	sqliteConn   *sql.Conn
	server       *Server
	connectionID uint32
	clientCaps   uint32
	statusFlags  uint16
	currentDB    string

	characterSetClient     string
	characterSetConnection string
	characterSetResults    string
	collationConnection    string

	nextStatementID uint32
	statements      map[uint32]*preparedStatement
}

type resultColumn struct {
	Name string
	Type byte
}

type resultSet struct {
	Columns []resultColumn
	Rows    [][]any
}

type okResult struct {
	AffectedRows uint64
	LastInsertID uint64
	Warnings     uint16
}

type mysqlError struct {
	Code     uint16
	SQLState string
	Message  string
}

func (e *mysqlError) Error() string {
	return e.Message
}

func (c *mysqlConn) serve(ctx context.Context) error {
	if err := c.writeHandshake(); err != nil {
		return err
	}
	payload, _, err := c.readPacket()
	if err != nil {
		return err
	}
	if err := c.readHandshakeResponse(payload); err != nil {
		_ = c.writeErr(2, errPacket(mysqlErrUnknown, "HY000", err.Error()))
		return err
	}
	if err := c.writeOK(2, okResult{}); err != nil {
		return err
	}

	for {
		payload, _, err := c.readPacket()
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			return errors.New("empty command packet")
		}

		switch payload[0] {
		case commandQuit:
			return nil
		case commandPing, commandResetConn:
			if err := c.writeOK(1, okResult{}); err != nil {
				return err
			}
		case commandInitDB:
			c.currentDB = string(payload[1:])
			if err := c.writeOK(1, okResult{}); err != nil {
				return err
			}
		case commandQuery:
			if err := c.handleQuery(ctx, string(payload[1:])); err != nil {
				return err
			}
		case commandStmtPrepare:
			if err := c.handleStmtPrepare(string(payload[1:])); err != nil {
				return err
			}
		case commandStmtExecute:
			if err := c.handleStmtExecute(ctx, payload[1:]); err != nil {
				return err
			}
		case commandStmtSendLong:
			if err := c.handleStmtSendLongData(payload[1:]); err != nil {
				return err
			}
		case commandStmtClose:
			c.handleStmtClose(payload[1:])
		case commandStmtReset:
			if err := c.handleStmtReset(payload[1:]); err != nil {
				return err
			}
		default:
			err := errPacket(mysqlErrUnknown, "HY000", fmt.Sprintf("Unsupported MySQL command: 0x%02x", payload[0]))
			if err := c.writeErr(1, err); err != nil {
				return err
			}
		}
	}
}

func (c *mysqlConn) readHandshakeResponse(payload []byte) error {
	if len(payload) < 4 {
		return errors.New("malformed handshake response")
	}
	c.clientCaps = binary.LittleEndian.Uint32(payload[:4])
	if c.clientCaps&clientProtocol41 == 0 {
		return errors.New("CLIENT_PROTOCOL_41 is required")
	}
	if len(payload) < 32 {
		return errors.New("malformed protocol 41 handshake response")
	}

	pos := 4 + 4 + 1 + 23
	if pos >= len(payload) {
		return nil
	}
	userEnd := bytes.IndexByte(payload[pos:], 0x00)
	if userEnd < 0 {
		return nil
	}
	pos += userEnd + 1

	if c.clientCaps&clientSecureConnection != 0 {
		if pos >= len(payload) {
			return nil
		}
		authLen := int(payload[pos])
		pos++
		if pos+authLen > len(payload) {
			return nil
		}
		pos += authLen
	} else {
		authEnd := bytes.IndexByte(payload[pos:], 0x00)
		if authEnd < 0 {
			return nil
		}
		pos += authEnd + 1
	}

	if c.clientCaps&clientConnectWithDB != 0 && pos < len(payload) {
		dbEnd := bytes.IndexByte(payload[pos:], 0x00)
		if dbEnd >= 0 {
			c.currentDB = string(payload[pos : pos+dbEnd])
		}
	}
	return nil
}

func (c *mysqlConn) writeHandshake() error {
	authData := []byte("12345678abcdefghijkl")
	caps := clientLongPassword |
		clientLongFlag |
		clientConnectWithDB |
		clientProtocol41 |
		clientTransactions |
		clientSecureConnection |
		clientPluginAuth

	payload := make([]byte, 0, 128)
	payload = append(payload, 0x0a)
	payload = append(payload, c.server.cfg.Server.MySQLVersion...)
	payload = append(payload, 0x00)
	payload = appendUint32(payload, c.connectionID)
	payload = append(payload, authData[:8]...)
	payload = append(payload, 0x00)
	payload = appendUint16(payload, uint16(caps))
	payload = append(payload, 45)
	payload = appendUint16(payload, c.statusFlags)
	payload = appendUint16(payload, uint16(caps>>16))
	payload = append(payload, byte(len(authData)+1))
	payload = append(payload, make([]byte, 10)...)
	payload = append(payload, authData[8:]...)
	payload = append(payload, 0x00)
	payload = append(payload, "mysql_native_password"...)
	payload = append(payload, 0x00)
	return c.writePacket(0, payload)
}

func (c *mysqlConn) handleQuery(ctx context.Context, sqlText string) error {
	resp, err := c.executeQuery(ctx, "COM_QUERY", sqlText)
	if err != nil {
		if errors.Is(err, errRuleDisconnect) {
			return err
		}
		var mysqlErr *mysqlError
		if errors.As(err, &mysqlErr) {
			return c.writeErr(1, mysqlErr)
		}
		if isSQLiteSyntaxError(err) {
			c.recordUnsupported("COM_QUERY", sqlText, normalizeSQL(sqlText), "sqlite")
		}
		return c.writeErr(1, mapSQLiteError(sqlText, err))
	}

	switch v := resp.(type) {
	case okResult:
		return c.writeOK(1, v)
	case resultSet:
		return c.writeResultSet(1, v)
	default:
		return c.writeErr(1, errPacket(mysqlErrUnknown, "HY000", "Unsupported query response"))
	}
}

func (c *mysqlConn) executeQuery(ctx context.Context, command, sqlText string, args ...any) (any, error) {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return nil, errPacket(mysqlErrUnknown, "HY000", "Unsupported query: empty SQL")
	}
	normalized := normalizeSQL(trimmed)
	upper := strings.ToUpper(normalized)

	if resp, matched, err := c.server.executeRule(ctx, sqlText, args); matched || err != nil {
		c.logQuery(command, "rules", sqlText, normalized)
		return resp, err
	}

	switch {
	case strings.HasPrefix(upper, "SET NAMES "):
		c.logQuery(command, "compat", sqlText, normalized)
		return c.setNames(normalized), nil
	case strings.HasPrefix(upper, "SET AUTOCOMMIT"):
		c.logQuery(command, "compat", sqlText, normalized)
		return c.setAutocommit(upper), nil
	case upper == "SELECT VERSION()" || upper == "SELECT VERSION":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("VERSION()", c.server.cfg.Server.MySQLVersion), nil
	case upper == "SELECT @@VERSION" || upper == "SELECT @@SESSION.VERSION" || upper == "SELECT @@GLOBAL.VERSION":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("@@version", c.server.cfg.Server.MySQLVersion), nil
	case strings.HasPrefix(upper, "SELECT @@"):
		return c.selectVariable(command, sqlText, normalized)
	case upper == "SHOW VARIABLES":
		c.logQuery(command, "compat", sqlText, normalized)
		return c.showVariables(), nil
	case upper == "SHOW TABLES":
		c.logQuery(command, "compat", sqlText, normalized)
		return c.showTables(ctx)
	case upper == "BEGIN" || upper == "START TRANSACTION":
		c.logQuery(command, "sqlite", sqlText, normalized)
		return c.execSQLite(ctx, "BEGIN")
	case upper == "COMMIT":
		c.logQuery(command, "sqlite", sqlText, normalized)
		resp, err := c.execSQLite(ctx, "COMMIT")
		c.statusFlags &^= serverStatusInTrans
		return resp, err
	case upper == "ROLLBACK":
		c.logQuery(command, "sqlite", sqlText, normalized)
		resp, err := c.execSQLite(ctx, "ROLLBACK")
		c.statusFlags &^= serverStatusInTrans
		return resp, err
	case strings.HasPrefix(upper, "SAVEPOINT ") ||
		strings.HasPrefix(upper, "RELEASE SAVEPOINT ") ||
		strings.HasPrefix(upper, "ROLLBACK TO SAVEPOINT "):
		c.logQuery(command, "sqlite", sqlText, normalized)
		return c.execSQLite(ctx, trimmed)
	}

	if isReadQuery(upper) {
		c.logQuery(command, "sqlite", sqlText, normalized)
		return c.querySQLite(ctx, translateSQL(trimmed), args...)
	}
	if isWriteQuery(upper) {
		c.logQuery(command, "sqlite", sqlText, normalized)
		return c.execSQLite(ctx, translateSQL(trimmed), args...)
	}

	c.recordUnsupported(command, sqlText, normalized, "unsupported")
	return nil, c.server.unsupportedError(sqlText)
}

func (c *mysqlConn) setAutocommit(upper string) okResult {
	if strings.Contains(upper, "= 0") || strings.HasSuffix(upper, "=0") || strings.HasSuffix(upper, " OFF") {
		c.statusFlags &^= serverStatusAutocommit
		return okResult{}
	}
	c.statusFlags |= serverStatusAutocommit
	return okResult{}
}

func (c *mysqlConn) setNames(normalizedSQL string) okResult {
	fields := strings.Fields(normalizedSQL)
	if len(fields) < 3 {
		return okResult{}
	}

	charset := unquoteSQLWord(fields[2])
	if strings.EqualFold(charset, "DEFAULT") {
		c.resetCharacterSetState()
		return okResult{}
	}

	c.characterSetClient = charset
	c.characterSetConnection = charset
	c.characterSetResults = charset
	for i := 3; i+1 < len(fields); i++ {
		if strings.EqualFold(fields[i], "COLLATE") {
			c.collationConnection = unquoteSQLWord(fields[i+1])
			break
		}
	}
	return okResult{}
}

func (c *mysqlConn) resetCharacterSetState() {
	c.characterSetClient = c.server.cfg.Compat.Variables["character_set_client"]
	c.characterSetConnection = c.server.cfg.Compat.Variables["character_set_connection"]
	c.characterSetResults = c.server.cfg.Compat.Variables["character_set_results"]
	c.collationConnection = c.server.cfg.Compat.Variables["collation_connection"]
}

func (c *mysqlConn) selectVariable(command, sqlText, normalizedSQL string) (resultSet, error) {
	expr := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(normalizedSQL), "SELECT"))
	expr = strings.TrimSuffix(expr, ";")
	name := strings.TrimSpace(expr)
	name = strings.TrimPrefix(name, "@@")
	name = strings.ToLower(name)
	name = strings.TrimPrefix(name, "session.")
	name = strings.TrimPrefix(name, "global.")

	value, ok := c.compatVariable(name)
	if !ok {
		c.recordUnsupported(command, sqlText, normalizedSQL, "compat")
		return resultSet{}, c.server.unsupportedError(normalizedSQL)
	}
	c.logQuery(command, "compat", sqlText, normalizedSQL)
	return resultSet{
		Columns: []resultColumn{{Name: expr, Type: fieldTypeVarString}},
		Rows:    [][]any{{value}},
	}, nil
}

func (c *mysqlConn) showVariables() resultSet {
	variables := c.compatVariables()
	keys := make([]string, 0, len(variables))
	for k := range variables {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rows := make([][]any, 0, len(keys))
	for _, k := range keys {
		rows = append(rows, []any{k, variables[k]})
	}
	return resultSet{
		Columns: []resultColumn{
			{Name: "Variable_name", Type: fieldTypeVarString},
			{Name: "Value", Type: fieldTypeVarString},
		},
		Rows: rows,
	}
}

func (c *mysqlConn) compatVariable(name string) (string, bool) {
	switch name {
	case "autocommit":
		if c.statusFlags&serverStatusAutocommit != 0 {
			return "1", true
		}
		return "0", true
	case "character_set_client":
		return c.characterSetClient, true
	case "character_set_connection":
		return c.characterSetConnection, true
	case "character_set_results":
		return c.characterSetResults, true
	case "collation_connection":
		return c.collationConnection, true
	default:
		value, ok := c.server.cfg.Compat.Variables[name]
		return value, ok
	}
}

func (c *mysqlConn) compatVariables() map[string]string {
	variables := make(map[string]string, len(c.server.cfg.Compat.Variables))
	for k, v := range c.server.cfg.Compat.Variables {
		variables[k] = v
	}
	for _, name := range []string{
		"autocommit",
		"character_set_client",
		"character_set_connection",
		"character_set_results",
		"collation_connection",
	} {
		value, _ := c.compatVariable(name)
		variables[name] = value
	}
	return variables
}

func (c *mysqlConn) showTables(ctx context.Context) (resultSet, error) {
	rs, err := c.querySQLite(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return resultSet{}, err
	}
	if len(rs.Columns) == 1 {
		rs.Columns[0].Name = "Tables_in_" + c.currentDB
	}
	return rs, nil
}

func (c *mysqlConn) execSQLite(ctx context.Context, query string, args ...any) (okResult, error) {
	if strings.EqualFold(strings.TrimSpace(query), "BEGIN") {
		c.statusFlags |= serverStatusInTrans
	}
	res, err := c.sqliteConn.ExecContext(ctx, query, args...)
	if err != nil {
		return okResult{}, err
	}
	affected, _ := res.RowsAffected()
	lastID, _ := res.LastInsertId()
	return okResult{AffectedRows: uint64NonNegative(affected), LastInsertID: uint64NonNegative(lastID)}, nil
}

func (c *mysqlConn) querySQLite(ctx context.Context, query string, args ...any) (resultSet, error) {
	rows, err := c.sqliteConn.QueryContext(ctx, query, args...)
	if err != nil {
		return resultSet{}, err
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return resultSet{}, err
	}
	columnTypes, _ := rows.ColumnTypes()
	columns := make([]resultColumn, len(names))
	for i, name := range names {
		columns[i] = resultColumn{Name: name, Type: fieldTypeVarString}
		if i < len(columnTypes) {
			columns[i].Type = mysqlFieldType(columnTypes[i])
		}
	}

	resultRows := make([][]any, 0)
	for rows.Next() {
		values := make([]any, len(names))
		scan := make([]any, len(names))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return resultSet{}, err
		}
		for i, value := range values {
			if columns[i].Type == fieldTypeVarString {
				columns[i].Type = fieldTypeFromValue(value)
			}
		}
		resultRows = append(resultRows, values)
	}
	if err := rows.Err(); err != nil {
		return resultSet{}, err
	}
	return resultSet{Columns: columns, Rows: resultRows}, nil
}

func (c *mysqlConn) writeResultSet(seq byte, rs resultSet) error {
	if err := c.writePacket(seq, appendLenEncInt(nil, uint64(len(rs.Columns)))); err != nil {
		return err
	}
	seq++
	for _, col := range rs.Columns {
		if err := c.writePacket(seq, columnDefinition(c.currentDB, col)); err != nil {
			return err
		}
		seq++
	}
	if err := c.writeEOF(seq); err != nil {
		return err
	}
	seq++
	for _, row := range rs.Rows {
		if err := c.writePacket(seq, textRow(row)); err != nil {
			return err
		}
		seq++
	}
	return c.writeEOF(seq)
}

func (c *mysqlConn) writeBinaryResultSet(seq byte, rs resultSet) error {
	rows := make([][]byte, len(rs.Rows))
	for i, row := range rs.Rows {
		payload, err := binaryRow(rs.Columns, row)
		if err != nil {
			return err
		}
		rows[i] = payload
	}

	if err := c.writePacket(seq, appendLenEncInt(nil, uint64(len(rs.Columns)))); err != nil {
		return err
	}
	seq++
	for _, col := range rs.Columns {
		if err := c.writePacket(seq, columnDefinition(c.currentDB, col)); err != nil {
			return err
		}
		seq++
	}
	if err := c.writeEOF(seq); err != nil {
		return err
	}
	seq++
	for _, payload := range rows {
		if err := c.writePacket(seq, payload); err != nil {
			return err
		}
		seq++
	}
	return c.writeEOF(seq)
}

func (c *mysqlConn) writeOK(seq byte, ok okResult) error {
	payload := []byte{0x00}
	payload = appendLenEncInt(payload, ok.AffectedRows)
	payload = appendLenEncInt(payload, ok.LastInsertID)
	payload = appendUint16(payload, c.statusFlags)
	payload = appendUint16(payload, ok.Warnings)
	return c.writePacket(seq, payload)
}

func (c *mysqlConn) writeEOF(seq byte) error {
	payload := []byte{0xfe}
	payload = appendUint16(payload, 0)
	payload = appendUint16(payload, c.statusFlags)
	return c.writePacket(seq, payload)
}

func (c *mysqlConn) writeErr(seq byte, err *mysqlError) error {
	payload := []byte{0xff}
	payload = appendUint16(payload, err.Code)
	payload = append(payload, '#')
	payload = append(payload, fixedSQLState(err.SQLState)...)
	payload = append(payload, err.Message...)
	return c.writePacket(seq, payload)
}

func (c *mysqlConn) readPacket() ([]byte, byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.netConn, header); err != nil {
		return nil, 0, err
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	seq := header[3]
	if length == 0 {
		return nil, seq, nil
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.netConn, payload); err != nil {
		return nil, seq, err
	}
	return payload, seq, nil
}

func (c *mysqlConn) writePacket(seq byte, payload []byte) error {
	if len(payload) >= 1<<24 {
		return errors.New("mysqlmock does not support packets larger than 16MB")
	}
	header := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), seq}
	if _, err := c.netConn.Write(header); err != nil {
		return err
	}
	_, err := c.netConn.Write(payload)
	return err
}

func oneRow(name string, value any) resultSet {
	return resultSet{
		Columns: []resultColumn{{Name: name, Type: fieldTypeVarString}},
		Rows:    [][]any{{value}},
	}
}

func columnDefinition(schema string, col resultColumn) []byte {
	payload := make([]byte, 0, 64)
	payload = appendLenEncString(payload, "def")
	payload = appendLenEncString(payload, schema)
	payload = appendLenEncString(payload, "")
	payload = appendLenEncString(payload, "")
	payload = appendLenEncString(payload, col.Name)
	payload = appendLenEncString(payload, col.Name)
	payload = append(payload, 0x0c)
	charset := uint16(45)
	if col.Type != fieldTypeVarString && col.Type != fieldTypeBlob {
		charset = 63
	}
	payload = appendUint16(payload, charset)
	payload = appendUint32(payload, 1024)
	payload = append(payload, col.Type)
	payload = appendUint16(payload, 0)
	payload = append(payload, 0)
	payload = append(payload, 0x00, 0x00)
	return payload
}

func textRow(values []any) []byte {
	payload := make([]byte, 0, len(values)*8)
	for _, value := range values {
		if value == nil {
			payload = append(payload, 0xfb)
			continue
		}
		payload = appendLenEncString(payload, textValue(value))
	}
	return payload
}

func textValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999999")
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprint(v)
	}
}

func mysqlFieldType(ct *sql.ColumnType) byte {
	dbType := strings.ToUpper(ct.DatabaseTypeName())
	switch {
	case strings.Contains(dbType, "INT"):
		return fieldTypeLongLong
	case dbType == "REAL" || dbType == "DOUBLE" || dbType == "FLOAT":
		return fieldTypeDouble
	case dbType == "NUMERIC" || dbType == "DECIMAL":
		return fieldTypeDecimal
	case strings.Contains(dbType, "BLOB"):
		return fieldTypeBlob
	case strings.Contains(dbType, "DATE") || strings.Contains(dbType, "TIME"):
		return fieldTypeDateTime
	default:
		return fieldTypeVarString
	}
}

func fieldTypeFromValue(value any) byte {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fieldTypeLongLong
	case float32, float64:
		return fieldTypeDouble
	case []byte:
		return fieldTypeBlob
	case time.Time:
		return fieldTypeDateTime
	default:
		return fieldTypeVarString
	}
}

func appendUint16(buf []byte, n uint16) []byte {
	return append(buf, byte(n), byte(n>>8))
}

func appendUint32(buf []byte, n uint32) []byte {
	return append(buf, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
}

func appendLenEncInt(buf []byte, n uint64) []byte {
	switch {
	case n < 251:
		return append(buf, byte(n))
	case n <= math.MaxUint16:
		buf = append(buf, 0xfc)
		return appendUint16(buf, uint16(n))
	case n <= 0x00ffffff:
		return append(buf, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	default:
		buf = append(buf, 0xfe)
		for i := 0; i < 8; i++ {
			buf = append(buf, byte(n>>(8*i)))
		}
		return buf
	}
}

func appendLenEncString(buf []byte, s string) []byte {
	buf = appendLenEncInt(buf, uint64(len(s)))
	return append(buf, s...)
}

func fixedSQLState(state string) string {
	if len(state) == 5 {
		return state
	}
	if state == "" {
		return "HY000"
	}
	if len(state) > 5 {
		return state[:5]
	}
	return state + strings.Repeat("0", 5-len(state))
}

func errPacket(code uint16, state, message string) *mysqlError {
	return &mysqlError{Code: code, SQLState: fixedSQLState(state), Message: message}
}

func mapSQLiteError(sqlText string, err error) *mysqlError {
	msg := err.Error()
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "unique constraint"):
		return errPacket(mysqlErrDupEntry, "23000", "Duplicate entry")
	case strings.Contains(lower, "not null constraint"):
		return errPacket(mysqlErrBadNull, "23000", "Column cannot be null")
	case strings.Contains(lower, "foreign key constraint"):
		return errPacket(mysqlErrNoReferenced, "23000", "Cannot add or update child row")
	case strings.Contains(lower, "constraint failed"):
		return errPacket(mysqlErrCheck, "HY000", "Check constraint failed")
	case strings.Contains(lower, "no such table"):
		return errPacket(mysqlErrNoSuchTable, "42S02", msg)
	case strings.Contains(lower, "syntax error"):
		return errPacket(mysqlErrUnknown, "HY000", "Unsupported query: "+sqlText)
	default:
		return errPacket(mysqlErrUnknown, "HY000", msg)
	}
}

func isSQLiteSyntaxError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "syntax error")
}

func isReadQuery(upper string) bool {
	return strings.HasPrefix(upper, "SELECT ") ||
		upper == "SELECT" ||
		strings.HasPrefix(upper, "WITH ") ||
		strings.HasPrefix(upper, "PRAGMA ")
}

func isWriteQuery(upper string) bool {
	return strings.HasPrefix(upper, "INSERT ") ||
		strings.HasPrefix(upper, "UPDATE ") ||
		strings.HasPrefix(upper, "DELETE ") ||
		strings.HasPrefix(upper, "REPLACE ")
}

func normalizeSQL(sqlText string) string {
	trimmed := strings.TrimSpace(sqlText)
	trimmed = strings.TrimSuffix(trimmed, ";")
	return strings.Join(strings.Fields(trimmed), " ")
}

func unquoteSQLWord(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, ";")
	if len(value) >= 2 {
		quote := value[0]
		if (quote == '\'' || quote == '"' || quote == '`') && value[len(value)-1] == quote {
			return value[1 : len(value)-1]
		}
	}
	return value
}

func translateSQL(sqlText string) string {
	replacer := strings.NewReplacer(
		"CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP",
		"current_timestamp()", "CURRENT_TIMESTAMP",
		"NOW()", "CURRENT_TIMESTAMP",
		"now()", "CURRENT_TIMESTAMP",
	)
	return replacer.Replace(sqlText)
}

func (c *mysqlConn) logQuery(command, route, sqlText, normalizedSQL string) {
	c.server.logQuery(queryLogEvent{
		Event:         "query",
		ConnectionID:  c.connectionID,
		Command:       command,
		Route:         route,
		Database:      c.currentDB,
		SQL:           sqlText,
		NormalizedSQL: normalizedSQL,
	})
}

func (c *mysqlConn) recordUnsupported(command, sqlText, normalizedSQL, routeStage string) {
	c.server.recordUnsupported(UnsupportedQuery{
		SQL:           sqlText,
		NormalizedSQL: normalizedSQL,
		ConnectionID:  c.connectionID,
		Command:       command,
		CurrentDB:     c.currentDB,
		RouteStage:    routeStage,
	})
}

func (s *Server) unsupportedError(sqlText string) *mysqlError {
	cfg := normalizedUnsupportedConfig(s.cfg.Fallback.Unsupported)
	return errPacket(cfg.Code, cfg.SQLState, cfg.Message+": "+sqlText)
}

func suggestedRule(sqlText string, unsupported UnsupportedConfig) string {
	unsupported = normalizedUnsupportedConfig(unsupported)
	quoted := strconv.Quote(sqlText)
	return "Suggested rule:\n" +
		"  - name: generated unsupported query\n" +
		"    request:\n" +
		"      match: exact\n" +
		"      sql: " + quoted + "\n" +
		"    response:\n" +
		"      type: error\n" +
		fmt.Sprintf("      code: %d\n", unsupported.Code) +
		"      sql_state: " + strconv.Quote(fixedSQLState(unsupported.SQLState)) + "\n" +
		"      message: " + strconv.Quote(unsupported.Message)
}

func normalizedUnsupportedConfig(cfg UnsupportedConfig) UnsupportedConfig {
	if cfg.Type == "" {
		cfg.Type = "error"
	}
	if cfg.Code == 0 {
		cfg.Code = mysqlErrUnknown
	}
	if cfg.SQLState == "" {
		cfg.SQLState = "HY000"
	}
	if cfg.Message == "" {
		cfg.Message = "Unsupported query"
	}
	return cfg
}

func uint64NonNegative(n int64) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}
