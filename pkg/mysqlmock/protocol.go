package mysqlmock

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
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
	clientDeprecateEOF     uint32 = 0x01000000

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

	informationSchemaCache informationSchemaCache
}

type resultColumn struct {
	Name          string
	Type          byte
	ZeroFillWidth int
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
	caps := serverCapabilityFlags()

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

func serverCapabilityFlags() uint32 {
	return clientLongPassword |
		clientLongFlag |
		clientConnectWithDB |
		clientProtocol41 |
		clientTransactions |
		clientSecureConnection |
		clientPluginAuth |
		clientDeprecateEOF
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
		return c.writeErr(1, c.mapSQLiteError(sqlText, err))
	}

	switch v := resp.(type) {
	case okResult:
		c.recordResult(v)
		return c.writeOK(1, v)
	case resultSet:
		c.recordResult(v)
		return c.writeResultSet(1, v)
	case *sqliteResultSet:
		c.recordResult(v)
		return c.writeSQLiteTextResultSet(1, v)
	default:
		return c.writeErr(1, errPacket(mysqlErrUnknown, "HY000", "Unsupported query response"))
	}
}

func (c *mysqlConn) executeQuery(ctx context.Context, command, sqlText string, args ...any) (any, error) {
	start := time.Now()
	route := ""
	normalized := ""
	defer func() {
		if route != "" {
			c.server.stats.recordQueryTiming(command, route, normalized, time.Since(start))
		}
	}()
	recordRoute := func(nextRoute string) {
		route = nextRoute
		c.logQuery(command, nextRoute, sqlText, normalized)
	}

	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return nil, errPacket(mysqlErrUnknown, "HY000", "Unsupported query: empty SQL")
	}
	normalized = normalizeSQL(trimmed)
	upper := strings.ToUpper(normalized)
	versionColumn, isVersionQuery := selectVersionColumnName(trimmed)

	if resp, matched, err := c.server.executeRule(ctx, sqlText, args); matched || err != nil {
		recordRoute("rules")
		return resp, err
	}

	switch {
	case strings.HasPrefix(upper, "SET NAMES "):
		recordRoute("compat")
		resp := c.setNames(normalized)
		c.setVariables(ctx, trimmed)
		return resp, nil
	case strings.HasPrefix(upper, "SET AUTOCOMMIT"):
		recordRoute("compat")
		return c.setAutocommit(upper), nil
	case strings.HasPrefix(upper, "SET TRANSACTION "):
		recordRoute("compat")
		return okResult{}, nil
	case strings.HasPrefix(upper, "SET "):
		recordRoute("compat")
		return c.setVariables(ctx, trimmed), nil
	case isVersionQuery:
		recordRoute("compat")
		return oneRow(versionColumn, c.server.cfg.Server.MySQLVersion), nil
	case isAdvisoryLockQuery(upper):
		recordRoute("compat")
		return c.advisoryLockResult(trimmed), nil
	case upper == "SELECT DATABASE()":
		recordRoute("compat")
		return oneRow("DATABASE()", c.currentDB), nil
	case upper == "SELECT SCHEMA()":
		recordRoute("compat")
		return oneRow("SCHEMA()", c.currentDB), nil
	case upper == "SELECT USER()":
		recordRoute("compat")
		return oneRow("USER()", c.currentUser()), nil
	case upper == "SELECT CURRENT_USER()" || upper == "SELECT CURRENT_USER":
		recordRoute("compat")
		return oneRow("CURRENT_USER()", c.currentUser()), nil
	case upper == "SELECT CONNECTION_ID()":
		recordRoute("compat")
		return oneRow("CONNECTION_ID()", c.connectionID), nil
	case upper == "SELECT LAST_INSERT_ID()":
		recordRoute("compat")
		return oneRow("LAST_INSERT_ID()", c.lastInsertID), nil
	case upper == "SELECT ROW_COUNT()":
		recordRoute("compat")
		return oneRow("ROW_COUNT()", c.lastAffectedRows), nil
	case upper == "SELECT @@VERSION" || upper == "SELECT @@SESSION.VERSION" || upper == "SELECT @@GLOBAL.VERSION":
		recordRoute("compat")
		return oneRow("@@version", c.server.cfg.Server.MySQLVersion), nil
	case strings.HasPrefix(upper, "SELECT @@"):
		route = "compat"
		return c.selectVariable(command, sqlText, normalized)
	case upper == "SHOW VARIABLES":
		recordRoute("compat")
		return c.showVariables(), nil
	case upper == "SHOW TABLES":
		recordRoute("compat")
		return c.showTables(ctx)
	case isShowFullFieldsQuery(upper):
		recordRoute("compat")
		return c.showFullFields(ctx, trimmed)
	case isShowCreateTableQuery(upper):
		recordRoute("compat")
		return c.showCreateTable(ctx, trimmed)
	case isShowKeysQuery(upper):
		recordRoute("compat")
		return c.showKeys(ctx, trimmed)
	case isInformationSchemaQuery(upper):
		recordRoute("compat")
		return c.queryInformationSchemaText(ctx, trimmed, command == "COM_QUERY", args...)
	case isCreateDatabaseStatement(trimmed):
		recordRoute("compat")
		return okResult{}, nil
	case upper == "BEGIN" || upper == "START TRANSACTION":
		recordRoute("sqlite")
		return c.execSQLite(ctx, "BEGIN")
	case upper == "COMMIT":
		recordRoute("sqlite")
		resp, err := c.execSQLite(ctx, "COMMIT")
		c.statusFlags &^= serverStatusInTrans
		return resp, err
	case upper == "ROLLBACK":
		recordRoute("sqlite")
		resp, err := c.execSQLite(ctx, "ROLLBACK")
		if err == nil {
			err = c.restoreMySQLAutoIncrementSequences(ctx)
		}
		c.statusFlags &^= serverStatusInTrans
		return resp, err
	case upper == "ROLLBACK AND CHAIN":
		recordRoute("sqlite")
		if _, err := c.execSQLite(ctx, "ROLLBACK"); err != nil {
			return okResult{}, err
		}
		if err := c.restoreMySQLAutoIncrementSequences(ctx); err != nil {
			return okResult{}, err
		}
		return c.execSQLite(ctx, "BEGIN")
	case strings.HasPrefix(upper, "ROLLBACK TO SAVEPOINT "):
		recordRoute("sqlite")
		resp, err := c.execSQLite(ctx, trimmed)
		if err == nil {
			err = c.restoreMySQLAutoIncrementSequences(ctx)
		}
		return resp, err
	case strings.HasPrefix(upper, "SAVEPOINT ") ||
		strings.HasPrefix(upper, "RELEASE SAVEPOINT "):
		recordRoute("sqlite")
		return c.execSQLite(ctx, trimmed)
	}

	if isReadQuery(upper) {
		recordRoute("sqlite")
		return c.querySQLiteText(ctx, c.server.translateSQLCached(trimmed), command == "COM_QUERY", args...)
	}
	if isWriteQuery(upper) {
		recordRoute("sqlite")
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
		resp, err := c.execSQLiteStatements(ctx, c.server.translateSQLStatementsCached(trimmed), args...)
		if err == nil {
			c.server.recordMySQLIndexMetadata(trimmed)
			c.server.recordMySQLColumnMetadata(trimmed)
			c.server.recordMySQLTableDDL(trimmed)
			c.server.invalidateMySQLTableDDLForStatement(trimmed)
		}
		return resp, err
	}

	route = "unsupported"
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

func selectVersionColumnName(sqlText string) (string, bool) {
	sqlText = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sqlText), ";"))
	pos := 0
	if !consumeKeyword(sqlText, &pos, "SELECT") {
		return "", false
	}
	if !consumeKeyword(sqlText, &pos, "VERSION") {
		return "", false
	}
	pos = skipSQLSpaces(sqlText, pos)
	if pos < len(sqlText) && sqlText[pos] == '(' {
		end, ok := parenthesizedSQLSpan(sqlText, pos)
		if !ok || strings.TrimSpace(sqlText[pos+1:end-1]) != "" {
			return "", false
		}
		pos = end
	}
	pos = skipSQLSpaces(sqlText, pos)
	if pos >= len(sqlText) {
		return "VERSION()", true
	}
	if consumeKeyword(sqlText, &pos, "AS") {
		alias, next, ok := readSQLNameToken(sqlText, pos)
		if !ok {
			return "", false
		}
		pos = skipSQLSpaces(sqlText, next)
		if pos != len(sqlText) {
			return "", false
		}
		return unquoteSQLWord(alias), true
	}
	alias, next, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return "", false
	}
	pos = skipSQLSpaces(sqlText, next)
	if pos != len(sqlText) {
		return "", false
	}
	return unquoteSQLWord(alias), true
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
	sqliteStart := time.Now()
	res, err := c.sqliteConn.ExecContext(ctx, query, args...)
	c.server.stats.recordPhaseTiming("sqlite.exec", time.Since(sqliteStart))
	if err != nil {
		return okResult{}, err
	}
	if isSchemaChangingQuery(query) {
		c.server.bumpSchemaVersion()
	}
	affected, _ := res.RowsAffected()
	lastID, _ := res.LastInsertId()
	result := okResult{AffectedRows: uint64NonNegative(affected), LastInsertID: uint64NonNegative(lastID)}
	c.recordMySQLAutoIncrementAllocation(ctx, query, result)
	return result, nil
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
	case *sqliteResultSet:
		c.lastAffectedRows = -1
	}
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

func (c *mysqlConn) resultZeroFillWidths(query string, names []string) []int {
	widths := make([]int, len(names))
	tableName, selectItems, ok := parseSimpleSelectSource(query)
	if !ok {
		return widths
	}
	if len(selectItems) == 1 && isSelectAllItem(selectItems[0]) {
		for i, name := range names {
			if metadata, ok := c.server.lookupMySQLColumnMetadata(tableName, name); ok {
				widths[i] = metadata.ZeroFillWidth
			}
		}
		return widths
	}
	if len(selectItems) != len(names) {
		return widths
	}
	for i, item := range selectItems {
		columnName, ok := selectItemColumnName(item)
		if !ok {
			continue
		}
		if metadata, ok := c.server.lookupMySQLColumnMetadata(tableName, columnName); ok {
			widths[i] = metadata.ZeroFillWidth
		}
	}
	return widths
}

func parseSimpleSelectTableName(sqlText string) (string, bool) {
	tableName, _, ok := parseSimpleSelectSource(sqlText)
	return tableName, ok
}

func parseSimpleSelectSource(sqlText string) (string, []string, bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "SELECT") {
		return "", nil, false
	}
	fromPos, ok := findTopLevelSQLKeyword(sqlText, pos, "FROM")
	if !ok {
		return "", nil, false
	}
	selectItems := splitSQLTopLevelList(sqlText[pos:fromPos])
	pos = fromPos
	if !consumeKeyword(sqlText, &pos, "FROM") {
		return "", nil, false
	}
	pos = skipSQLSpaces(sqlText, pos)
	if pos >= len(sqlText) || sqlText[pos] == '(' {
		return "", nil, false
	}
	tableName, pos, ok := readSQLQualifiedName(sqlText, pos)
	if !ok {
		return "", nil, false
	}
	if hasAdditionalTopLevelSelectSource(sqlText[pos:]) {
		return "", nil, false
	}
	return tableName, selectItems, true
}

func isSelectAllItem(item string) bool {
	item = strings.TrimSpace(item)
	if item == "*" {
		return true
	}
	if strings.HasSuffix(item, ".*") {
		prefix := strings.TrimSpace(strings.TrimSuffix(item, ".*"))
		_, pos, ok := readSQLQualifiedName(prefix, 0)
		return ok && skipSQLSpaces(prefix, pos) == len(prefix)
	}
	return false
}

func selectItemColumnName(item string) (string, bool) {
	columnName, pos, ok := readSQLQualifiedName(item, 0)
	if !ok {
		return "", false
	}
	pos = skipSQLSpaces(item, pos)
	if pos == len(item) {
		return columnName, true
	}
	if consumeKeyword(item, &pos, "AS") {
		_, pos, ok = readSQLNameToken(item, pos)
		if !ok {
			return "", false
		}
		return columnName, skipSQLSpaces(item, pos) == len(item)
	}
	if _, pos, ok = readSQLNameToken(item, pos); ok {
		return columnName, skipSQLSpaces(item, pos) == len(item)
	}
	return "", false
}

func hasAdditionalTopLevelSelectSource(sqlText string) bool {
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
			i++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			i++
			continue
		case ',':
			if depth == 0 {
				return true
			}
			i++
			continue
		}
		word, end, ok := readSQLIdentifier(sqlText, i)
		if !ok {
			i++
			continue
		}
		if depth == 0 {
			switch strings.ToUpper(word) {
			case "JOIN":
				return true
			case "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FOR", "LOCK":
				return false
			}
		}
		i = end
	}
	return false
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
		strings.HasPrefix(upper, "DROP DATABASE ") ||
		strings.HasPrefix(upper, "DROP SCHEMA ") ||
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

func translateSQL(sqlText string) string {
	if !needsSQLTranslation(sqlText) {
		return sqlText
	}
	if stripped, ok := stripMySQLLockingClause(sqlText); ok {
		sqlText = stripped
	}
	sqlText = translateMySQLUpdateSetTargets(sqlText)
	sqlText = translateMySQLLikeDefaultEscape(sqlText)
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
			case "ZEROFILL":
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
	return translateMySQLStringLiterals(out.String())
}

func needsSQLTranslation(sqlText string) bool {
	if strings.Contains(sqlText, "<=>") {
		return true
	}
	if hasMySQLBackslashEscapedString(sqlText) {
		return true
	}
	if canContainMySQLLikePredicate(sqlText) && hasSQLIdentifier(sqlText, "LIKE") {
		return true
	}
	if hasMySQLQualifiedUpdateSetTarget(sqlText) {
		return true
	}
	if stripsMySQLDDLOptions(sqlText) {
		return true
	}
	for i := 0; i < len(sqlText); {
		if end, ok := quotedSQLSpan(sqlText, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			i = end
			continue
		}
		ident, end, ok := readSQLIdentifier(sqlText, i)
		if !ok {
			i++
			continue
		}
		switch strings.ToUpper(ident) {
		case "TRUE", "FALSE", "NOW", "CURRENT_TIMESTAMP",
			"CONCAT", "DATE_FORMAT", "JSON_EXTRACT", "JSON_UNQUOTE", "CAST",
			"AUTO_INCREMENT", "AUTO_RANDOM", "FOR", "LOCK":
			return true
		}
		i = end
	}
	return false
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
