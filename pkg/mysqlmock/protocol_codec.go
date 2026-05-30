package mysqlmock

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

func (c *mysqlConn) writeResultSet(seq byte, rs resultSet) error {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("protocol.result_set_text", time.Since(start))
	}()

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
	nextSeq, err := c.writeColumnDefinitionTerminator(seq)
	if err != nil {
		return err
	}
	seq = nextSeq
	for _, row := range rs.Rows {
		if err := c.writePacket(seq, textRow(rs.Columns, row)); err != nil {
			return err
		}
		seq++
	}
	return c.writeResultSetTerminator(seq)
}

func (c *mysqlConn) writeSQLiteTextResultSet(seq byte, rs *sqliteResultSet) error {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("protocol.result_set_sqlite_text", time.Since(start))
	}()
	defer rs.Close()

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
	nextSeq, err := c.writeColumnDefinitionTerminator(seq)
	if err != nil {
		return err
	}
	seq = nextSeq
	for {
		row, ok, err := rs.nextRow(false)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := c.writePacket(seq, textRow(rs.Columns, row)); err != nil {
			return err
		}
		seq++
	}
	return c.writeResultSetTerminator(seq)
}

func (c *mysqlConn) writeBinaryResultSet(seq byte, rs resultSet) error {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("protocol.result_set_binary", time.Since(start))
	}()

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
	nextSeq, err := c.writeColumnDefinitionTerminator(seq)
	if err != nil {
		return err
	}
	seq = nextSeq
	for _, payload := range rows {
		if err := c.writePacket(seq, payload); err != nil {
			return err
		}
		seq++
	}
	return c.writeResultSetTerminator(seq)
}

func (c *mysqlConn) writeOK(seq byte, ok okResult) error {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("protocol.ok", time.Since(start))
	}()

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

func (c *mysqlConn) writeColumnDefinitionTerminator(seq byte) (byte, error) {
	if c.clientCaps&clientDeprecateEOF != 0 {
		return seq, nil
	}
	if err := c.writeEOF(seq); err != nil {
		return seq, err
	}
	return seq + 1, nil
}

func (c *mysqlConn) writeResultSetTerminator(seq byte) error {
	if c.clientCaps&clientDeprecateEOF != 0 {
		return c.writeEOFAsOK(seq)
	}
	return c.writeEOF(seq)
}

func (c *mysqlConn) writeEOFAsOK(seq byte) error {
	payload := []byte{0xfe}
	payload = appendLenEncInt(payload, 0)
	payload = appendLenEncInt(payload, 0)
	payload = appendUint16(payload, c.statusFlags)
	payload = appendUint16(payload, 0)
	return c.writePacket(seq, payload)
}

func (c *mysqlConn) writeErr(seq byte, err *mysqlError) error {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("protocol.err", time.Since(start))
	}()

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

func textRow(columns []resultColumn, values []any) []byte {
	payload := make([]byte, 0, len(values)*8)
	for i, value := range values {
		if value == nil {
			payload = append(payload, 0xfb)
			continue
		}
		var column resultColumn
		if i < len(columns) {
			column = columns[i]
		}
		payload = appendLenEncString(payload, textValueForColumn(value, column))
	}
	return payload
}

func textValueForColumn(value any, column resultColumn) string {
	if column.ZeroFillWidth <= 0 {
		return textValue(value)
	}
	text := textValue(value)
	if len(text) >= column.ZeroFillWidth || strings.HasPrefix(text, "-") {
		return text
	}
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return text
		}
	}
	return strings.Repeat("0", column.ZeroFillWidth-len(text)) + text
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

func mysqlZeroFillWidth(databaseTypeName string) int {
	upper := strings.ToUpper(strings.TrimSpace(databaseTypeName))
	if !strings.Contains(upper, "ZEROFILL") {
		return 0
	}
	open := strings.IndexByte(upper, '(')
	if open < 0 {
		return 0
	}
	close := strings.IndexByte(upper[open+1:], ')')
	if close < 0 {
		return 0
	}
	width, err := parseInt64String(upper[open+1 : open+1+close])
	if err != nil || width <= 0 {
		return 0
	}
	return int(width)
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

func (c *mysqlConn) mapSQLiteError(sqlText string, err error) *mysqlError {
	if c.server.cfg.Compat.WriteValidation == "off" {
		return errPacket(mysqlErrUnknown, "HY000", err.Error())
	}
	return mapSQLiteError(sqlText, err)
}

func isSQLiteSyntaxError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "syntax error")
}
