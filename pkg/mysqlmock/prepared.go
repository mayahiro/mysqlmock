package mysqlmock

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type preparedStatement struct {
	ID         uint32
	SQL        string
	ParamCount int
	ParamTypes []preparedParamType
	LongData   map[int][]byte
}

type preparedParamType struct {
	FieldType byte
	Unsigned  bool
}

func (c *mysqlConn) handleStmtPrepare(sqlText string) error {
	c.server.logf("connection=%d command=COM_STMT_PREPARE sql=%q", c.connectionID, sqlText)

	paramCount := countPlaceholders(sqlText)
	if paramCount > math.MaxUint16 {
		err := errPacket(mysqlErrUnknown, "HY000", "Prepared statement has too many parameters")
		return c.writeErr(1, err)
	}

	stmtID := c.nextStatementID
	c.nextStatementID++
	stmt := &preparedStatement{
		ID:         stmtID,
		SQL:        sqlText,
		ParamCount: paramCount,
		LongData:   map[int][]byte{},
	}
	c.statements[stmtID] = stmt

	payload := []byte{0x00}
	payload = appendUint32(payload, stmtID)
	payload = appendUint16(payload, 0)
	payload = appendUint16(payload, uint16(paramCount))
	payload = append(payload, 0x00)
	payload = appendUint16(payload, 0)
	if err := c.writePacket(1, payload); err != nil {
		return err
	}

	seq := byte(2)
	for i := range paramCount {
		col := resultColumn{Name: fmt.Sprintf("param%d", i+1), Type: fieldTypeVarString}
		if err := c.writePacket(seq, columnDefinition(c.currentDB, col)); err != nil {
			return err
		}
		seq++
	}
	if paramCount > 0 {
		return c.writeEOF(seq)
	}
	return nil
}

func (c *mysqlConn) handleStmtExecute(ctx context.Context, payload []byte) error {
	if len(payload) < 9 {
		return c.writeErr(1, errPacket(mysqlErrUnknown, "HY000", "Malformed COM_STMT_EXECUTE packet"))
	}

	stmtID := binary.LittleEndian.Uint32(payload[:4])
	stmt, ok := c.statements[stmtID]
	if !ok {
		return c.writeErr(1, errPacket(mysqlErrUnknown, "HY000", fmt.Sprintf("Unknown prepared statement id: %d", stmtID)))
	}
	c.server.logf("connection=%d command=COM_STMT_EXECUTE statement_id=%d sql=%q", c.connectionID, stmtID, stmt.SQL)

	args, err := stmt.decodeExecuteArgs(payload)
	stmt.LongData = map[int][]byte{}
	if err != nil {
		return c.writeErr(1, errPacket(mysqlErrUnknown, "HY000", err.Error()))
	}

	resp, err := c.executeQuery(ctx, "COM_STMT_EXECUTE", stmt.SQL, args...)
	if err != nil {
		if errors.Is(err, errRuleDisconnect) {
			return err
		}
		var mysqlErr *mysqlError
		if errors.As(err, &mysqlErr) {
			return c.writeErr(1, mysqlErr)
		}
		if isSQLiteSyntaxError(err) {
			c.recordUnsupported("COM_STMT_EXECUTE", stmt.SQL, normalizeSQL(stmt.SQL), "sqlite")
		}
		return c.writeErr(1, mapSQLiteError(stmt.SQL, err))
	}

	switch v := resp.(type) {
	case okResult:
		c.recordResult(v)
		return c.writeOK(1, v)
	case resultSet:
		c.recordResult(v)
		if err := c.writeBinaryResultSet(1, v); err != nil {
			return c.writeErr(1, errPacket(mysqlErrUnknown, "HY000", err.Error()))
		}
		return nil
	default:
		return c.writeErr(1, errPacket(mysqlErrUnknown, "HY000", "Unsupported query response"))
	}
}

func (c *mysqlConn) handleStmtSendLongData(payload []byte) error {
	if len(payload) < 6 {
		return errors.New("malformed COM_STMT_SEND_LONG_DATA packet")
	}
	stmtID := binary.LittleEndian.Uint32(payload[:4])
	paramID := int(binary.LittleEndian.Uint16(payload[4:6]))
	stmt, ok := c.statements[stmtID]
	if !ok {
		return nil
	}
	stmt.LongData[paramID] = append(stmt.LongData[paramID], payload[6:]...)
	return nil
}

func (c *mysqlConn) handleStmtClose(payload []byte) {
	if len(payload) < 4 {
		return
	}
	stmtID := binary.LittleEndian.Uint32(payload[:4])
	delete(c.statements, stmtID)
}

func (c *mysqlConn) handleStmtReset(payload []byte) error {
	if len(payload) < 4 {
		return c.writeErr(1, errPacket(mysqlErrUnknown, "HY000", "Malformed COM_STMT_RESET packet"))
	}
	stmtID := binary.LittleEndian.Uint32(payload[:4])
	stmt, ok := c.statements[stmtID]
	if !ok {
		return c.writeErr(1, errPacket(mysqlErrUnknown, "HY000", fmt.Sprintf("Unknown prepared statement id: %d", stmtID)))
	}
	stmt.LongData = map[int][]byte{}
	return c.writeOK(1, okResult{})
}

func (stmt *preparedStatement) decodeExecuteArgs(payload []byte) ([]any, error) {
	pos := 9
	if stmt.ParamCount == 0 {
		return nil, nil
	}

	nullLen := (stmt.ParamCount + 7) / 8
	if len(payload) < pos+nullLen+1 {
		return nil, errors.New("malformed COM_STMT_EXECUTE parameter header")
	}
	nullBitmap := payload[pos : pos+nullLen]
	pos += nullLen

	newParamsBound := payload[pos]
	pos++
	if newParamsBound == 1 {
		if len(payload) < pos+stmt.ParamCount*2 {
			return nil, errors.New("malformed COM_STMT_EXECUTE parameter types")
		}
		stmt.ParamTypes = make([]preparedParamType, stmt.ParamCount)
		for i := range stmt.ParamCount {
			stmt.ParamTypes[i] = preparedParamType{
				FieldType: payload[pos+i*2],
				Unsigned:  payload[pos+i*2+1]&0x80 != 0,
			}
		}
		pos += stmt.ParamCount * 2
	} else if len(stmt.ParamTypes) != stmt.ParamCount {
		return nil, errors.New("COM_STMT_EXECUTE omitted parameter types before any types were bound")
	}

	values := payload[pos:]
	args := make([]any, stmt.ParamCount)
	valuePos := 0
	for i := range stmt.ParamCount {
		if nullBitmap[i/8]&(1<<uint(i&7)) != 0 {
			args[i] = nil
			continue
		}
		if data, ok := stmt.LongData[i]; ok {
			args[i] = append([]byte(nil), data...)
			continue
		}

		value, n, err := decodePreparedValue(values[valuePos:], stmt.ParamTypes[i])
		if err != nil {
			return nil, fmt.Errorf("decode parameter %d: %w", i+1, err)
		}
		valuePos += n
		args[i] = value
	}
	if valuePos != len(values) {
		return nil, errors.New("malformed COM_STMT_EXECUTE parameter values")
	}
	return args, nil
}

func decodePreparedValue(data []byte, typ preparedParamType) (any, int, error) {
	switch typ.FieldType {
	case fieldTypeNull:
		return nil, 0, nil
	case fieldTypeTiny:
		if len(data) < 1 {
			return nil, 0, ioShortBuffer()
		}
		if typ.Unsigned {
			return int64(data[0]), 1, nil
		}
		return int64(int8(data[0])), 1, nil
	case fieldTypeShort, fieldTypeYear:
		if len(data) < 2 {
			return nil, 0, ioShortBuffer()
		}
		n := binary.LittleEndian.Uint16(data[:2])
		if typ.Unsigned {
			return int64(n), 2, nil
		}
		return int64(int16(n)), 2, nil
	case fieldTypeLong, fieldTypeInt24:
		if len(data) < 4 {
			return nil, 0, ioShortBuffer()
		}
		n := binary.LittleEndian.Uint32(data[:4])
		if typ.Unsigned {
			return int64(n), 4, nil
		}
		return int64(int32(n)), 4, nil
	case fieldTypeLongLong:
		if len(data) < 8 {
			return nil, 0, ioShortBuffer()
		}
		n := binary.LittleEndian.Uint64(data[:8])
		if typ.Unsigned {
			if n <= math.MaxInt64 {
				return int64(n), 8, nil
			}
			return strconv.FormatUint(n, 10), 8, nil
		}
		return int64(n), 8, nil
	case fieldTypeFloat:
		if len(data) < 4 {
			return nil, 0, ioShortBuffer()
		}
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(data[:4]))), 4, nil
	case fieldTypeDouble:
		if len(data) < 8 {
			return nil, 0, ioShortBuffer()
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data[:8])), 8, nil
	case fieldTypeDecimal, fieldTypeNewDec, fieldTypeVarChar, fieldTypeVarString, fieldTypeString,
		fieldTypeEnum, fieldTypeSet:
		value, n, err := readLenEncBytes(data)
		if err != nil {
			return nil, 0, err
		}
		return string(value), n, nil
	case fieldTypeBlob, fieldTypeTinyBlob, fieldTypeMedBlob, fieldTypeLongBlob,
		fieldTypeBit, fieldTypeJSON, fieldTypeGeometry:
		value, n, err := readLenEncBytes(data)
		if err != nil {
			return nil, 0, err
		}
		return append([]byte(nil), value...), n, nil
	case fieldTypeDate, fieldTypeNewDate, fieldTypeDateTime, fieldTypeTimestamp:
		value, n, err := decodeBinaryDateTime(data)
		if err != nil {
			return nil, 0, err
		}
		return value, n, nil
	case fieldTypeTime:
		value, n, err := decodeBinaryTime(data)
		if err != nil {
			return nil, 0, err
		}
		return value, n, nil
	default:
		return nil, 0, fmt.Errorf("unsupported prepared statement parameter type: 0x%02x", typ.FieldType)
	}
}

func binaryRow(columns []resultColumn, values []any) ([]byte, error) {
	if len(values) != len(columns) {
		return nil, fmt.Errorf("binary row value count mismatch: got %d, want %d", len(values), len(columns))
	}
	nullLen := (len(columns) + 7 + 2) / 8
	payload := make([]byte, 1+nullLen)
	payload[0] = 0x00

	var err error
	for i, value := range values {
		if value == nil {
			payload[1+(i+2)/8] |= 1 << uint((i+2)&7)
			continue
		}
		payload, err = appendBinaryValue(payload, columns[i].Type, value)
		if err != nil {
			return nil, fmt.Errorf("encode column %q: %w", columns[i].Name, err)
		}
	}
	return payload, nil
}

func appendBinaryValue(buf []byte, typ byte, value any) ([]byte, error) {
	switch typ {
	case fieldTypeTiny:
		n, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		return append(buf, byte(n)), nil
	case fieldTypeShort, fieldTypeYear:
		n, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		return appendUint16(buf, uint16(n)), nil
	case fieldTypeLong, fieldTypeInt24:
		n, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		return binary.LittleEndian.AppendUint32(buf, uint32(n)), nil
	case fieldTypeLongLong:
		n, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		return binary.LittleEndian.AppendUint64(buf, uint64(n)), nil
	case fieldTypeFloat:
		n, err := toFloat64(value)
		if err != nil {
			return nil, err
		}
		return binary.LittleEndian.AppendUint32(buf, math.Float32bits(float32(n))), nil
	case fieldTypeDouble:
		n, err := toFloat64(value)
		if err != nil {
			return nil, err
		}
		return binary.LittleEndian.AppendUint64(buf, math.Float64bits(n)), nil
	case fieldTypeDate, fieldTypeNewDate, fieldTypeDateTime, fieldTypeTimestamp:
		return appendBinaryDateTime(buf, value)
	case fieldTypeDecimal, fieldTypeNewDec, fieldTypeVarChar, fieldTypeVarString, fieldTypeString,
		fieldTypeEnum, fieldTypeSet,
		fieldTypeBlob, fieldTypeTinyBlob, fieldTypeMedBlob, fieldTypeLongBlob,
		fieldTypeBit, fieldTypeJSON, fieldTypeGeometry:
		return appendLenEncBytes(buf, []byte(textValue(value))), nil
	default:
		return appendLenEncBytes(buf, []byte(textValue(value))), nil
	}
}

func countPlaceholders(sqlText string) int {
	count := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	lineComment := false
	blockComment := false
	escaped := false

	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		next := byte(0)
		if i+1 < len(sqlText) {
			next = sqlText[i+1]
		}

		if lineComment {
			if ch == '\n' || ch == '\r' {
				lineComment = false
			}
			continue
		}
		if blockComment {
			if ch == '*' && next == '/' {
				blockComment = false
				i++
			}
			continue
		}
		if inSingle {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		switch {
		case ch == '-' && next == '-':
			lineComment = true
			i++
		case ch == '#':
			lineComment = true
		case ch == '/' && next == '*':
			blockComment = true
			i++
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '`':
			inBacktick = true
		case ch == '?':
			count++
		}
	}
	return count
}

func readLenEncBytes(data []byte) ([]byte, int, error) {
	n, pos, isNull, err := readLenEncInt(data)
	if err != nil {
		return nil, 0, err
	}
	if isNull {
		return nil, pos, nil
	}
	if n > uint64(len(data)-pos) {
		return nil, 0, ioShortBuffer()
	}
	end := pos + int(n)
	return data[pos:end], end, nil
}

func readLenEncInt(data []byte) (uint64, int, bool, error) {
	if len(data) == 0 {
		return 0, 0, false, ioShortBuffer()
	}
	switch data[0] {
	case 0xfb:
		return 0, 1, true, nil
	case 0xfc:
		if len(data) < 3 {
			return 0, 0, false, ioShortBuffer()
		}
		return uint64(binary.LittleEndian.Uint16(data[1:3])), 3, false, nil
	case 0xfd:
		if len(data) < 4 {
			return 0, 0, false, ioShortBuffer()
		}
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16, 4, false, nil
	case 0xfe:
		if len(data) < 9 {
			return 0, 0, false, ioShortBuffer()
		}
		return binary.LittleEndian.Uint64(data[1:9]), 9, false, nil
	default:
		if data[0] < 0xfb {
			return uint64(data[0]), 1, false, nil
		}
		return 0, 0, false, fmt.Errorf("invalid length-encoded integer marker: 0x%02x", data[0])
	}
}

func appendLenEncBytes(buf, value []byte) []byte {
	buf = appendLenEncInt(buf, uint64(len(value)))
	return append(buf, value...)
}

func decodeBinaryDateTime(data []byte) (time.Time, int, error) {
	if len(data) < 1 {
		return time.Time{}, 0, ioShortBuffer()
	}
	length := int(data[0])
	if len(data) < 1+length {
		return time.Time{}, 0, ioShortBuffer()
	}
	if length == 0 {
		return time.Time{}, 1, nil
	}
	if length != 4 && length != 7 && length != 11 {
		return time.Time{}, 0, fmt.Errorf("invalid binary datetime length: %d", length)
	}

	year := int(binary.LittleEndian.Uint16(data[1:3]))
	month := time.Month(data[3])
	day := int(data[4])
	hour, minute, second, micro := 0, 0, 0, 0
	if length >= 7 {
		hour = int(data[5])
		minute = int(data[6])
		second = int(data[7])
	}
	if length == 11 {
		micro = int(binary.LittleEndian.Uint32(data[8:12]))
	}
	return time.Date(year, month, day, hour, minute, second, micro*1000, time.Local), 1 + length, nil
}

func decodeBinaryTime(data []byte) (string, int, error) {
	if len(data) < 1 {
		return "", 0, ioShortBuffer()
	}
	length := int(data[0])
	if len(data) < 1+length {
		return "", 0, ioShortBuffer()
	}
	if length == 0 {
		return "00:00:00", 1, nil
	}
	if length != 8 && length != 12 {
		return "", 0, fmt.Errorf("invalid binary time length: %d", length)
	}
	sign := ""
	if data[1] == 1 {
		sign = "-"
	}
	days := binary.LittleEndian.Uint32(data[2:6])
	hours := uint32(data[6]) + days*24
	minutes := data[7]
	seconds := data[8]
	if length == 12 {
		micro := binary.LittleEndian.Uint32(data[9:13])
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hours, minutes, seconds, micro), 13, nil
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds), 9, nil
}

func appendBinaryDateTime(buf []byte, value any) ([]byte, error) {
	t, err := asTime(value)
	if err != nil {
		return nil, err
	}
	if t.IsZero() {
		return append(buf, 0x00), nil
	}
	micro := t.Nanosecond() / 1000
	if micro > 0 {
		buf = append(buf, 0x0b)
		buf = appendUint16(buf, uint16(t.Year()))
		buf = append(buf, byte(t.Month()), byte(t.Day()), byte(t.Hour()), byte(t.Minute()), byte(t.Second()))
		return binary.LittleEndian.AppendUint32(buf, uint32(micro)), nil
	}
	buf = append(buf, 0x07)
	buf = appendUint16(buf, uint16(t.Year()))
	return append(buf, byte(t.Month()), byte(t.Day()), byte(t.Hour()), byte(t.Minute()), byte(t.Second())), nil
}

func asTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		return parseDateTimeString(v)
	case []byte:
		return parseDateTimeString(string(v))
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to datetime", value)
	}
}

func parseDateTimeString(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "0000-00-00" || strings.HasPrefix(value, "0000-00-00 ") {
		return time.Time{}, nil
	}
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, value, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse datetime value %q", value)
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return 0, fmt.Errorf("integer value overflows int64: %d", v)
		}
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("integer value overflows int64: %d", v)
		}
		return int64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to integer", value)
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", value)
	}
}

func ioShortBuffer() error {
	return errors.New("packet too short")
}
