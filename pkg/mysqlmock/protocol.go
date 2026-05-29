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
	fieldTypeNewDate   byte = 0x0e
	fieldTypeVarChar   byte = 0x0f
	fieldTypeBit       byte = 0x10
	fieldTypeJSON      byte = 0xf5
	fieldTypeNewDec    byte = 0xf6
	fieldTypeEnum      byte = 0xf7
	fieldTypeSet       byte = 0xf8
	fieldTypeTinyBlob  byte = 0xf9
	fieldTypeMedBlob   byte = 0xfa
	fieldTypeLongBlob  byte = 0xfb
	fieldTypeBlob      byte = 0xfc
	fieldTypeVarString byte = 0xfd
	fieldTypeString    byte = 0xfe
	fieldTypeGeometry  byte = 0xff

	mysqlErrUnknown            uint16 = 1105
	mysqlErrNoSuchTable        uint16 = 1146
	mysqlErrDupEntry           uint16 = 1062
	mysqlErrBadNull            uint16 = 1048
	mysqlErrNoReferenced       uint16 = 1452
	mysqlErrWrongValue         uint16 = 1292
	mysqlErrWrongValueForField uint16 = 1366
	mysqlErrDataTooLong        uint16 = 1406
	mysqlErrCheck              uint16 = 3819
)

type mysqlConn struct {
	netConn      net.Conn
	sqliteConn   *sql.Conn
	server       *Server
	connectionID uint32
	user         string
	clientCaps   uint32
	statusFlags  uint16
	currentDB    string

	characterSetClient     string
	characterSetConnection string
	characterSetResults    string
	collationConnection    string
	sessionVariables       map[string]string

	lastInsertID     uint64
	lastAffectedRows int64

	nextStatementID uint32
	statements      map[uint32]*preparedStatement

	informationSchemaFullLoaded  bool
	informationSchemaFullVersion uint64
	informationSchemaTableCache  map[string]informationSchemaTableCacheEntry
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
	c.user = string(payload[pos : pos+userEnd])
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
		c.recordResult(v)
		return c.writeOK(1, v)
	case resultSet:
		c.recordResult(v)
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
		resp := c.setNames(normalized)
		c.setVariables(ctx, trimmed)
		return resp, nil
	case strings.HasPrefix(upper, "SET AUTOCOMMIT"):
		c.logQuery(command, "compat", sqlText, normalized)
		return c.setAutocommit(upper), nil
	case strings.HasPrefix(upper, "SET TRANSACTION "):
		c.logQuery(command, "compat", sqlText, normalized)
		return okResult{}, nil
	case strings.HasPrefix(upper, "SET "):
		c.logQuery(command, "compat", sqlText, normalized)
		return c.setVariables(ctx, trimmed), nil
	case upper == "SELECT VERSION()" || upper == "SELECT VERSION":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("VERSION()", c.server.cfg.Server.MySQLVersion), nil
	case isAdvisoryLockQuery(upper):
		c.logQuery(command, "compat", sqlText, normalized)
		return c.advisoryLockResult(trimmed), nil
	case upper == "SELECT DATABASE()":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("DATABASE()", c.currentDB), nil
	case upper == "SELECT SCHEMA()":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("SCHEMA()", c.currentDB), nil
	case upper == "SELECT USER()":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("USER()", c.currentUser()), nil
	case upper == "SELECT CURRENT_USER()" || upper == "SELECT CURRENT_USER":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("CURRENT_USER()", c.currentUser()), nil
	case upper == "SELECT CONNECTION_ID()":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("CONNECTION_ID()", c.connectionID), nil
	case upper == "SELECT LAST_INSERT_ID()":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("LAST_INSERT_ID()", c.lastInsertID), nil
	case upper == "SELECT ROW_COUNT()":
		c.logQuery(command, "compat", sqlText, normalized)
		return oneRow("ROW_COUNT()", c.lastAffectedRows), nil
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
	case isShowFullFieldsQuery(upper):
		c.logQuery(command, "compat", sqlText, normalized)
		return c.showFullFields(ctx, trimmed)
	case isShowCreateTableQuery(upper):
		c.logQuery(command, "compat", sqlText, normalized)
		return c.showCreateTable(ctx, trimmed)
	case isShowKeysQuery(upper):
		c.logQuery(command, "compat", sqlText, normalized)
		return c.showKeys(ctx, trimmed)
	case isInformationSchemaQuery(upper):
		c.logQuery(command, "compat", sqlText, normalized)
		return c.queryInformationSchema(ctx, trimmed, args...)
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
	case upper == "ROLLBACK AND CHAIN":
		c.logQuery(command, "sqlite", sqlText, normalized)
		if _, err := c.execSQLite(ctx, "ROLLBACK"); err != nil {
			return okResult{}, err
		}
		return c.execSQLite(ctx, "BEGIN")
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
		if resp, handled, err := c.execMySQLDDLCompatibility(ctx, trimmed); handled || err != nil {
			return resp, err
		}
		if strings.HasPrefix(upper, "INSERT ") {
			if resp, handled, err := c.execMySQLUpsert(ctx, trimmed, args...); handled || err != nil {
				return resp, err
			}
		}
		if strings.HasPrefix(upper, "INSERT ") || strings.HasPrefix(upper, "REPLACE ") {
			if resp, handled, err := c.execMySQLInsertCompatibility(ctx, trimmed, args...); handled || err != nil {
				return resp, err
			}
		}
		resp, err := c.execSQLiteStatements(ctx, translateSQLStatements(trimmed), args...)
		if err == nil {
			c.server.recordMySQLIndexMetadata(trimmed)
			c.server.recordMySQLTableDDL(trimmed)
			c.server.invalidateMySQLTableDDLForStatement(trimmed)
		}
		return resp, err
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

func (c *mysqlConn) setVariables(ctx context.Context, sqlText string) okResult {
	if c.sessionVariables == nil {
		c.sessionVariables = map[string]string{}
	}
	rest := strings.TrimSpace(sqlText[len("SET"):])
	for _, assignment := range splitSQLTopLevelList(rest) {
		name, value, ok := parseSetVariableAssignment(assignment)
		if !ok {
			continue
		}
		name = normalizeSystemVariableName(name)
		if name == "" {
			continue
		}
		value = c.normalizeSystemVariableValue(name, value)
		c.sessionVariables[name] = value
		if strings.EqualFold(name, "foreign_key_checks") {
			pragma := "ON"
			if value == "0" || strings.EqualFold(value, "OFF") {
				pragma = "OFF"
				if c.statusFlags&serverStatusInTrans != 0 {
					_, _ = c.sqliteConn.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON")
				}
			}
			_, _ = c.sqliteConn.ExecContext(ctx, "PRAGMA foreign_keys = "+pragma)
		}
	}
	return okResult{}
}

func parseSetVariableAssignment(assignment string) (name, value string, ok bool) {
	assignment = strings.TrimSpace(assignment)
	eq := topLevelEqualIndex(assignment)
	if eq < 0 {
		return "", "", false
	}
	name = strings.TrimSpace(assignment[:eq])
	value = strings.TrimSpace(strings.TrimSuffix(assignment[eq+1:], ";"))
	if name == "" || value == "" {
		return "", "", false
	}
	return name, value, true
}

func topLevelEqualIndex(sqlText string) int {
	depth := 0
	for i := 0; i < len(sqlText); {
		if end, ok := quotedSQLSpan(sqlText, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			i = end
			continue
		}
		switch sqlText[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '=':
			if depth == 0 {
				return i
			}
		}
		i++
	}
	return -1
}

func normalizeSystemVariableName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.TrimPrefix(name, "@@")
	name = strings.TrimPrefix(strings.ToLower(name), "session.")
	name = strings.TrimPrefix(name, "global.")
	return name
}

func (c *mysqlConn) normalizeSystemVariableValue(name, value string) string {
	value = strings.TrimSpace(strings.TrimSuffix(value, ";"))
	if strings.EqualFold(value, "DEFAULT") {
		if configured, ok := c.server.cfg.Compat.Variables[name]; ok {
			return configured
		}
		return ""
	}
	return unquoteSQLWord(value)
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
		if value, ok := c.sessionVariables[name]; ok {
			return value, true
		}
		value, ok := c.server.cfg.Compat.Variables[name]
		return value, ok
	}
}

func (c *mysqlConn) compatVariables() map[string]string {
	variables := make(map[string]string, len(c.server.cfg.Compat.Variables))
	for k, v := range c.server.cfg.Compat.Variables {
		variables[k] = v
	}
	for k, v := range c.sessionVariables {
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
	if err := c.validateMySQLWriteValues(ctx, query, args...); err != nil {
		return okResult{}, err
	}
	res, err := c.sqliteConn.ExecContext(ctx, query, args...)
	if err != nil {
		return okResult{}, err
	}
	if isSchemaChangingQuery(query) {
		c.server.bumpSchemaVersion()
	}
	affected, _ := res.RowsAffected()
	lastID, _ := res.LastInsertId()
	return okResult{AffectedRows: uint64NonNegative(affected), LastInsertID: uint64NonNegative(lastID)}, nil
}

func (c *mysqlConn) execSQLiteStatements(ctx context.Context, queries []string, args ...any) (okResult, error) {
	if len(queries) == 0 {
		return okResult{}, nil
	}
	if len(queries) == 1 {
		return c.execSQLite(ctx, queries[0], args...)
	}
	if len(args) > 0 {
		return okResult{}, errPacket(mysqlErrUnknown, "HY000", "Unsupported parameterized multi-statement translation")
	}

	var result okResult
	for _, query := range queries {
		if strings.TrimSpace(query) == "" {
			continue
		}
		next, err := c.execSQLite(ctx, query)
		if err != nil {
			return okResult{}, err
		}
		result = next
	}
	return result, nil
}

func (c *mysqlConn) recordResult(resp any) {
	switch v := resp.(type) {
	case okResult:
		c.lastAffectedRows = int64(v.AffectedRows)
		if v.LastInsertID != 0 {
			c.lastInsertID = v.LastInsertID
		}
	case resultSet:
		c.lastAffectedRows = -1
	}
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

func (c *mysqlConn) currentUser() string {
	user := c.user
	if user == "" {
		user = "mysqlmock"
	}
	return user + "@%"
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
	case strings.Contains(lower, "datatype mismatch"):
		return errPacket(mysqlErrWrongValueForField, "HY000", msg)
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
		strings.HasPrefix(upper, "REPLACE ") ||
		strings.HasPrefix(upper, "CREATE TABLE ") ||
		strings.HasPrefix(upper, "CREATE TEMPORARY TABLE ") ||
		strings.HasPrefix(upper, "CREATE TEMP TABLE ") ||
		strings.HasPrefix(upper, "CREATE INDEX ") ||
		strings.HasPrefix(upper, "CREATE UNIQUE INDEX ") ||
		strings.HasPrefix(upper, "ALTER TABLE ") ||
		strings.HasPrefix(upper, "RENAME TABLE ") ||
		strings.HasPrefix(upper, "DROP TABLE ") ||
		strings.HasPrefix(upper, "DROP INDEX ")
}

func isSchemaChangingQuery(sqlText string) bool {
	upper := strings.ToUpper(normalizeSQL(sqlText))
	return strings.HasPrefix(upper, "CREATE TABLE ") ||
		strings.HasPrefix(upper, "CREATE TEMPORARY TABLE ") ||
		strings.HasPrefix(upper, "CREATE TEMP TABLE ") ||
		strings.HasPrefix(upper, "CREATE VIEW ") ||
		strings.HasPrefix(upper, "CREATE INDEX ") ||
		strings.HasPrefix(upper, "CREATE UNIQUE INDEX ") ||
		strings.HasPrefix(upper, "ALTER TABLE ") ||
		strings.HasPrefix(upper, "RENAME TABLE ") ||
		strings.HasPrefix(upper, "DROP TABLE ") ||
		strings.HasPrefix(upper, "DROP VIEW ") ||
		strings.HasPrefix(upper, "DROP INDEX ")
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
	if stripped, ok := stripMySQLLockingClause(sqlText); ok {
		sqlText = stripped
	}
	sqlText = translateMySQLOperators(sqlText)
	sqlText = translateMySQLFunctionCalls(sqlText)

	if translated, ok := translateMySQLAlterTableAddIndex(sqlText); ok {
		return translated
	}
	if translated, ok := translateMySQLCreateIndex(sqlText); ok {
		return translated
	}

	var out strings.Builder
	out.Grow(len(sqlText))
	stripDDLMode := stripsMySQLDDLOptions(sqlText)

	for i := 0; i < len(sqlText); {
		if copyEnd, ok := quotedSQLSpan(sqlText, i); ok {
			out.WriteString(sqlText[i:copyEnd])
			i = copyEnd
			continue
		}
		if copyEnd, ok := sqlCommentSpan(sqlText, i); ok {
			if replacement, handled := translateTiDBDDLComment(sqlText[i:copyEnd]); stripDDLMode && handled {
				out.WriteString(replacement)
				i = copyEnd
				continue
			}
			out.WriteString(sqlText[i:copyEnd])
			i = copyEnd
			continue
		}
		if isSQLIdentifierStart(sqlText[i]) {
			start := i
			i++
			for i < len(sqlText) && isSQLIdentifierPart(sqlText[i]) {
				i++
			}
			ident := sqlText[start:i]
			upper := strings.ToUpper(ident)
			switch upper {
			case "TRUE":
				out.WriteByte('1')
			case "FALSE":
				out.WriteByte('0')
			case "AUTO_INCREMENT":
				if next := consumeMySQLAssignedOptionValue(sqlText, i); stripDDLMode && next >= 0 {
					i = next
				} else {
					out.WriteString("AUTOINCREMENT")
				}
			case "AUTO_RANDOM":
				if stripDDLMode {
					if next := consumeOptionalParenthesizedCall(sqlText, i); next >= 0 {
						i = next
					}
					out.WriteString("AUTOINCREMENT")
				} else {
					out.WriteString(ident)
				}
			case "UNSIGNED":
				if !stripDDLMode {
					out.WriteString(ident)
				}
			case "CLUSTERED", "NONCLUSTERED":
				if !stripDDLMode {
					out.WriteString(ident)
				}
			case "AUTO_RANDOM_BASE":
				if next := consumeMySQLAssignedOptionValue(sqlText, i); stripDDLMode && next >= 0 {
					i = next
				} else {
					out.WriteString(ident)
				}
			case "ENGINE", "CHARSET", "COLLATE", "COMMENT", "USING",
				"AVG_ROW_LENGTH", "CHECKSUM", "DELAY_KEY_WRITE", "INSERT_METHOD",
				"KEY_BLOCK_SIZE", "MAX_ROWS", "MIN_ROWS", "PACK_KEYS",
				"ROW_FORMAT", "STATS_AUTO_RECALC", "STATS_PERSISTENT":
				if next := consumeMySQLOptionValue(sqlText, i); stripDDLMode && next >= 0 {
					i = next
				} else {
					out.WriteString(ident)
				}
			case "ON":
				if next := consumeOnUpdateOption(sqlText, i); stripDDLMode && next >= 0 {
					i = next
				} else {
					out.WriteString(ident)
				}
			case "VISIBLE", "INVISIBLE":
				if !stripDDLMode {
					out.WriteString(ident)
				}
			case "CHARACTER":
				if next := consumeCharacterSetOption(sqlText, i); stripDDLMode && next >= 0 {
					i = next
				} else {
					out.WriteString(ident)
				}
			case "DEFAULT":
				if next := consumeDefaultCharsetOption(sqlText, i); stripDDLMode && next >= 0 {
					i = next
				} else {
					out.WriteString(ident)
				}
			case "NOW", "CURRENT_TIMESTAMP":
				if next := consumeCurrentTimestampCall(sqlText, i); next >= 0 {
					out.WriteString("CURRENT_TIMESTAMP")
					i = next
				} else {
					out.WriteString(ident)
				}
			default:
				out.WriteString(ident)
			}
			continue
		}
		out.WriteByte(sqlText[i])
		i++
	}
	return out.String()
}

func stripsMySQLDDLOptions(sqlText string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sqlText))
	return strings.HasPrefix(upper, "CREATE TABLE ") ||
		strings.HasPrefix(upper, "CREATE TEMPORARY TABLE ") ||
		strings.HasPrefix(upper, "CREATE TEMP TABLE ") ||
		strings.HasPrefix(upper, "ALTER TABLE ") ||
		strings.HasPrefix(upper, "CREATE INDEX ") ||
		strings.HasPrefix(upper, "CREATE UNIQUE INDEX ")
}

func translateMySQLAlterTableAddIndex(sqlText string) (string, bool) {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "ALTER") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", false
	}
	tableName, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "ADD") {
		return "", false
	}

	unique := consumeKeyword(sqlText, &pos, "UNIQUE")
	if !consumeKeyword(sqlText, &pos, "INDEX") && !consumeKeyword(sqlText, &pos, "KEY") {
		return "", false
	}
	if next, ok := consumeSQLNamedOption(sqlText, pos, "USING"); ok {
		pos = next
	}

	indexName, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return "", false
	}
	visibleIndexName := unquoteSQLWord(indexName)
	if next, ok := consumeSQLNamedOption(sqlText, pos, "USING"); ok {
		pos = next
	}

	columnsStart := skipSQLSpaces(sqlText, pos)
	columnsEnd, ok := parenthesizedSQLSpan(sqlText, columnsStart)
	if !ok {
		return "", false
	}
	columns := sqlText[columnsStart:columnsEnd]
	pos = columnsEnd
	for {
		if consumeKeyword(sqlText, &pos, "VISIBLE") || consumeKeyword(sqlText, &pos, "INVISIBLE") {
			pos = skipSQLSpaces(sqlText, pos)
			continue
		}
		next, ok := consumeSQLNamedOption(sqlText, pos, "USING")
		if !ok {
			break
		}
		pos = next
	}
	pos = skipSQLSpaces(sqlText, pos)
	if pos < len(sqlText) && sqlText[pos] == ';' {
		pos = skipSQLSpaces(sqlText, pos+1)
	}
	if pos != len(sqlText) {
		return "", false
	}

	var out strings.Builder
	out.WriteString("CREATE ")
	if unique {
		out.WriteString("UNIQUE ")
	}
	out.WriteString("INDEX ")
	out.WriteString(quoteIdent(sqliteIndexName(tableName, visibleIndexName)))
	out.WriteString(" ON ")
	out.WriteString(tableName)
	out.WriteByte(' ')
	out.WriteString(translateMySQLIndexColumns(columns))
	return out.String(), true
}

func translateMySQLCreateIndex(sqlText string) (string, bool) {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "CREATE") {
		return "", false
	}
	unique := consumeKeyword(sqlText, &pos, "UNIQUE")
	if !consumeKeyword(sqlText, &pos, "INDEX") {
		return "", false
	}
	indexName, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return "", false
	}
	if next, ok := consumeSQLNamedOption(sqlText, pos, "USING"); ok {
		pos = next
	}
	if !consumeKeyword(sqlText, &pos, "ON") {
		return "", false
	}
	visibleIndexName := unquoteSQLWord(indexName)
	tableName, pos, ok := readSQLQualifiedName(sqlText, pos)
	if !ok {
		return "", false
	}
	columnsStart := skipSQLSpaces(sqlText, pos)
	columnsEnd, ok := parenthesizedSQLSpan(sqlText, columnsStart)
	if !ok {
		return "", false
	}
	pos = skipSQLSpaces(sqlText, columnsEnd)
	for {
		if consumeKeyword(sqlText, &pos, "VISIBLE") || consumeKeyword(sqlText, &pos, "INVISIBLE") {
			pos = skipSQLSpaces(sqlText, pos)
			continue
		}
		next, ok := consumeSQLNamedOption(sqlText, pos, "USING")
		if !ok {
			break
		}
		pos = next
	}
	pos = skipSQLSpaces(sqlText, pos)
	if pos < len(sqlText) && sqlText[pos] == ';' {
		pos = skipSQLSpaces(sqlText, pos+1)
	}
	if pos != len(sqlText) {
		return "", false
	}

	var out strings.Builder
	out.WriteString("CREATE ")
	if unique {
		out.WriteString("UNIQUE ")
	}
	out.WriteString("INDEX ")
	out.WriteString(quoteIdent(sqliteIndexName(tableName, visibleIndexName)))
	out.WriteString(" ON ")
	out.WriteString(tableName)
	out.WriteByte(' ')
	out.WriteString(translateMySQLIndexColumns(sqlText[columnsStart:columnsEnd]))
	return out.String(), true
}

func consumeMySQLOptionValue(sqlText string, pos int) int {
	i := skipSQLSpaces(sqlText, pos)
	if i < len(sqlText) && sqlText[i] == '=' {
		i = skipSQLSpaces(sqlText, i+1)
	}
	return consumeSQLValue(sqlText, i)
}

func consumeMySQLAssignedOptionValue(sqlText string, pos int) int {
	i := skipSQLSpaces(sqlText, pos)
	if i >= len(sqlText) || sqlText[i] != '=' {
		return -1
	}
	return consumeSQLValue(sqlText, i+1)
}

func consumeCharacterSetOption(sqlText string, pos int) int {
	word, next, ok := readSQLIdentifier(sqlText, skipSQLSpaces(sqlText, pos))
	if !ok || !strings.EqualFold(word, "SET") {
		return -1
	}
	return consumeMySQLOptionValue(sqlText, next)
}

func consumeDefaultCharsetOption(sqlText string, pos int) int {
	word, next, ok := readSQLIdentifier(sqlText, skipSQLSpaces(sqlText, pos))
	if !ok {
		return -1
	}
	switch strings.ToUpper(word) {
	case "CHARSET":
		return consumeMySQLOptionValue(sqlText, next)
	case "CHARACTER":
		return consumeCharacterSetOption(sqlText, next)
	default:
		return -1
	}
}

func consumeSQLNamedOption(sqlText string, pos int, name string) (int, bool) {
	word, next, ok := readSQLIdentifier(sqlText, skipSQLSpaces(sqlText, pos))
	if !ok || !strings.EqualFold(word, name) {
		return pos, false
	}
	end := consumeMySQLOptionValue(sqlText, next)
	if end < 0 {
		return pos, false
	}
	return end, true
}

func consumeOnUpdateOption(sqlText string, pos int) int {
	word, next, ok := readSQLIdentifier(sqlText, skipSQLSpaces(sqlText, pos))
	if !ok || !strings.EqualFold(word, "UPDATE") {
		return -1
	}
	return consumeSQLValue(sqlText, next)
}

func consumeSQLValue(sqlText string, pos int) int {
	i := skipSQLSpaces(sqlText, pos)
	if copyEnd, ok := quotedSQLSpan(sqlText, i); ok {
		return copyEnd
	}
	if _, end, ok := readSQLIdentifier(sqlText, i); ok {
		if callEnd := consumeEmptyCall(sqlText, end); callEnd >= 0 {
			return callEnd
		}
		if callEnd := consumeOptionalNumericCall(sqlText, end); callEnd >= 0 {
			return callEnd
		}
		return end
	}
	if i < len(sqlText) && ('0' <= sqlText[i] && sqlText[i] <= '9') {
		i++
		for i < len(sqlText) && (('0' <= sqlText[i] && sqlText[i] <= '9') || sqlText[i] == '.') {
			i++
		}
		return i
	}
	return -1
}

func consumeCurrentTimestampCall(sqlText string, pos int) int {
	if next := consumeEmptyCall(sqlText, pos); next >= 0 {
		return next
	}
	return consumeOptionalNumericCall(sqlText, pos)
}

func consumeOptionalNumericCall(sqlText string, pos int) int {
	i := skipSQLSpaces(sqlText, pos)
	if i >= len(sqlText) || sqlText[i] != '(' {
		return -1
	}
	i = skipSQLSpaces(sqlText, i+1)
	start := i
	for i < len(sqlText) && '0' <= sqlText[i] && sqlText[i] <= '9' {
		i++
	}
	if i == start {
		return -1
	}
	i = skipSQLSpaces(sqlText, i)
	if i >= len(sqlText) || sqlText[i] != ')' {
		return -1
	}
	return i + 1
}

func consumeKeyword(sqlText string, pos *int, keyword string) bool {
	word, next, ok := readSQLIdentifier(sqlText, skipSQLSpaces(sqlText, *pos))
	if !ok || !strings.EqualFold(word, keyword) {
		return false
	}
	*pos = next
	return true
}

func readSQLNameToken(sqlText string, pos int) (string, int, bool) {
	pos = skipSQLSpaces(sqlText, pos)
	if pos >= len(sqlText) {
		return "", pos, false
	}
	if sqlText[pos] == '`' || sqlText[pos] == '"' {
		end, ok := quotedSQLSpan(sqlText, pos)
		if !ok {
			return "", pos, false
		}
		return sqlText[pos:end], end, true
	}
	if _, end, ok := readSQLIdentifier(sqlText, pos); ok {
		return sqlText[pos:end], end, true
	}
	return "", pos, false
}

func parenthesizedSQLSpan(sqlText string, pos int) (int, bool) {
	if pos >= len(sqlText) || sqlText[pos] != '(' {
		return pos, false
	}
	depth := 0
	for i := pos; i < len(sqlText); {
		if copyEnd, ok := quotedSQLSpan(sqlText, i); ok {
			i = copyEnd
			continue
		}
		if copyEnd, ok := sqlCommentSpan(sqlText, i); ok {
			i = copyEnd
			continue
		}
		switch sqlText[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1, true
			}
		}
		i++
	}
	return len(sqlText), false
}

func readSQLIdentifier(sqlText string, pos int) (string, int, bool) {
	if pos >= len(sqlText) || !isSQLIdentifierStart(sqlText[pos]) {
		return "", pos, false
	}
	start := pos
	pos++
	for pos < len(sqlText) && isSQLIdentifierPart(sqlText[pos]) {
		pos++
	}
	return sqlText[start:pos], pos, true
}

func skipSQLSpaces(sqlText string, pos int) int {
	for pos < len(sqlText) && isSQLSpace(sqlText[pos]) {
		pos++
	}
	return pos
}

func quotedSQLSpan(sqlText string, pos int) (int, bool) {
	if pos >= len(sqlText) {
		return pos, false
	}
	quote := sqlText[pos]
	if quote != '\'' && quote != '"' && quote != '`' {
		return pos, false
	}
	i := pos + 1
	for i < len(sqlText) {
		if sqlText[i] == '\\' && quote != '`' && i+1 < len(sqlText) {
			i += 2
			continue
		}
		if sqlText[i] == quote {
			if quote != '`' && i+1 < len(sqlText) && sqlText[i+1] == quote {
				i += 2
				continue
			}
			return i + 1, true
		}
		i++
	}
	return len(sqlText), true
}

func sqlCommentSpan(sqlText string, pos int) (int, bool) {
	if pos+1 < len(sqlText) && sqlText[pos] == '-' && sqlText[pos+1] == '-' {
		i := pos + 2
		for i < len(sqlText) && sqlText[i] != '\n' {
			i++
		}
		if i < len(sqlText) {
			i++
		}
		return i, true
	}
	if sqlText[pos] == '#' {
		i := pos + 1
		for i < len(sqlText) && sqlText[i] != '\n' {
			i++
		}
		if i < len(sqlText) {
			i++
		}
		return i, true
	}
	if pos+1 < len(sqlText) && sqlText[pos] == '/' && sqlText[pos+1] == '*' {
		i := pos + 2
		for i+1 < len(sqlText) && !(sqlText[i] == '*' && sqlText[i+1] == '/') {
			i++
		}
		if i+1 < len(sqlText) {
			i += 2
		} else {
			i = len(sqlText)
		}
		return i, true
	}
	return pos, false
}

func consumeEmptyCall(sqlText string, pos int) int {
	i := pos
	for i < len(sqlText) && isSQLSpace(sqlText[i]) {
		i++
	}
	if i >= len(sqlText) || sqlText[i] != '(' {
		return -1
	}
	i++
	for i < len(sqlText) && isSQLSpace(sqlText[i]) {
		i++
	}
	if i >= len(sqlText) || sqlText[i] != ')' {
		return -1
	}
	return i + 1
}

func isSQLIdentifierStart(ch byte) bool {
	return ch == '_' || ('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z')
}

func isSQLIdentifierPart(ch byte) bool {
	return isSQLIdentifierStart(ch) || ('0' <= ch && ch <= '9') || ch == '$'
}

func isSQLSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '\f':
		return true
	default:
		return false
	}
}

func (c *mysqlConn) logQuery(command, route, sqlText, normalizedSQL string) {
	c.server.logQuery(QueryEvent{
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

func (s *Server) suggestedRule(u UnsupportedQuery) string {
	if suggestion, ok := s.suggestVariableRule(u); ok {
		return suggestion
	}
	return suggestedErrorRule(u.SQL, s.cfg.Fallback.Unsupported)
}

func (s *Server) suggestVariableRule(u UnsupportedQuery) (string, bool) {
	expr, name, ok := unsupportedSelectVariable(u.NormalizedSQL)
	if !ok {
		return "", false
	}
	value := "TODO"
	if compatValue, ok := s.cfg.Compat.Variables[name]; ok {
		value = compatValue
	}
	return "Suggested rule:\n" +
		"  - name: generated unsupported query\n" +
		"    request:\n" +
		"      match: exact\n" +
		"      sql: " + strconv.Quote(u.NormalizedSQL) + "\n" +
		"    response:\n" +
		"      type: result_set\n" +
		"      columns:\n" +
		"        - name: " + strconv.Quote(expr) + "\n" +
		"          type: VARCHAR\n" +
		"      rows:\n" +
		"        - [" + strconv.Quote(value) + "]", true
}

func unsupportedSelectVariable(normalizedSQL string) (expr, name string, ok bool) {
	expr = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(normalizedSQL), "SELECT"))
	expr = strings.TrimSuffix(expr, ";")
	if !strings.HasPrefix(strings.TrimSpace(expr), "@@") {
		return "", "", false
	}
	name = strings.TrimSpace(expr)
	name = strings.TrimPrefix(name, "@@")
	name = strings.ToLower(name)
	name = strings.TrimPrefix(name, "session.")
	name = strings.TrimPrefix(name, "global.")
	if name == "" {
		return "", "", false
	}
	return expr, name, true
}

func suggestedErrorRule(sqlText string, unsupported UnsupportedConfig) string {
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
