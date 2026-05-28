package mysqlmock

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (c *mysqlConn) validateMySQLWriteValues(ctx context.Context, query string, args ...any) error {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil
	}
	upper := strings.ToUpper(normalizeSQL(trimmed))
	switch {
	case strings.HasPrefix(upper, "INSERT INTO "):
		return c.validateMySQLInsertValues(ctx, trimmed, args...)
	case strings.HasPrefix(upper, "UPDATE "):
		return c.validateMySQLUpdateValues(ctx, trimmed, args...)
	default:
		return nil
	}
}

func (c *mysqlConn) validateMySQLInsertValues(ctx context.Context, query string, args ...any) error {
	pos := 0
	if !consumeKeyword(query, &pos, "INSERT") {
		return nil
	}
	if !consumeKeyword(query, &pos, "INTO") {
		return nil
	}
	tableName, pos, ok := readSQLNameToken(query, pos)
	if !ok {
		return nil
	}
	columnsStart := skipSQLSpaces(query, pos)
	if columnsStart >= len(query) || query[columnsStart] != '(' {
		return nil
	}
	columnsEnd, ok := parenthesizedSQLSpan(query, columnsStart)
	if !ok {
		return nil
	}
	columns := []string{}
	for _, item := range splitSQLTopLevelList(query[columnsStart+1 : columnsEnd-1]) {
		item = strings.TrimSpace(item)
		if item != "" {
			columns = append(columns, unquoteSQLWord(item))
		}
	}
	pos = columnsEnd
	if !consumeKeyword(query, &pos, "VALUES") && !consumeKeyword(query, &pos, "VALUE") {
		return nil
	}

	tableColumns, err := c.tableColumns(ctx, tableName)
	if err != nil {
		return err
	}
	rows, _, ok := parseMySQLValuesRows(query, pos, args)
	if !ok {
		return nil
	}
	for rowIndex, row := range rows {
		values, err := c.evaluateUpsertRow(ctx, tableName, columns, tableColumns, row)
		if err != nil {
			return nil
		}
		for i, value := range values {
			if i >= len(columns) {
				continue
			}
			if err := validateMySQLColumnValue(tableColumns, columns[i], value, rowIndex+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *mysqlConn) validateMySQLUpdateValues(ctx context.Context, query string, args ...any) error {
	pos := 0
	if !consumeKeyword(query, &pos, "UPDATE") {
		return nil
	}
	tableName, pos, ok := readSQLNameToken(query, pos)
	if !ok {
		return nil
	}
	if !consumeKeyword(query, &pos, "SET") {
		return nil
	}
	setStart := pos
	setEnd := len(query)
	for i := pos; i < len(query); {
		if end, ok := quotedSQLSpan(query, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(query, i); ok {
			i = end
			continue
		}
		word, end, ok := readSQLIdentifier(query, i)
		if ok && strings.EqualFold(word, "WHERE") {
			setEnd = i
			break
		}
		if ok {
			i = end
			continue
		}
		i++
	}
	tableColumns, err := c.tableColumns(ctx, tableName)
	if err != nil {
		return err
	}
	argPos := 0
	for _, assignment := range splitSQLTopLevelList(query[setStart:setEnd]) {
		eq := topLevelEqualIndex(assignment)
		if eq < 0 {
			argPos += countPlaceholders(assignment)
			continue
		}
		columnName := strings.TrimSpace(assignment[:eq])
		valueExpr := assignment[eq+1:]
		placeholderCount := countPlaceholders(valueExpr)
		switch {
		case placeholderCount == 1 && isBarePlaceholderExpr(valueExpr) && argPos < len(args):
			if err := validateMySQLColumnValue(tableColumns, unquoteSQLWord(columnName), args[argPos], 1); err != nil {
				return err
			}
		case placeholderCount == 0:
			value, ok := c.evaluateValidationExpr(ctx, valueExpr)
			if ok {
				if err := validateMySQLColumnValue(tableColumns, unquoteSQLWord(columnName), value, 1); err != nil {
					return err
				}
			}
		}
		argPos += placeholderCount
	}
	return nil
}

func isBarePlaceholderExpr(expr string) bool {
	expr = strings.TrimSpace(expr)
	return expr == "?"
}

func (c *mysqlConn) evaluateValidationExpr(ctx context.Context, expr string) (any, bool) {
	rs, err := c.querySQLite(ctx, "SELECT "+translateSQL(strings.TrimSpace(expr)))
	if err != nil || len(rs.Rows) != 1 || len(rs.Rows[0]) != 1 {
		return nil, false
	}
	return rs.Rows[0][0], true
}

func validateMySQLColumnValue(columns []sqliteTableColumn, columnName string, value any, rowNumber int) error {
	if value == nil {
		return nil
	}
	column, ok := findSQLiteTableColumn(columns, columnName)
	if !ok {
		return nil
	}
	if maxLength, ok := mysqlCharacterColumnLength(column.Type); ok {
		if text, ok := stringLikeValue(value); ok && len([]rune(text)) > maxLength {
			return errPacket(mysqlErrDataTooLong, "22001", fmt.Sprintf("Data too long for column '%s' at row %d", column.Name, rowNumber))
		}
	}
	if isMySQLIntegerType(column.Type) {
		if text, ok := stringLikeValue(value); ok && !isIntegerLiteral(strings.TrimSpace(text)) {
			return errPacket(mysqlErrWrongValueForField, "HY000", fmt.Sprintf("Incorrect integer value: '%s' for column '%s' at row %d", text, column.Name, rowNumber))
		}
	}
	if isMySQLDateTimeType(column.Type) {
		if text, ok := stringLikeValue(value); ok && !isMySQLDateTimeLiteral(strings.TrimSpace(text)) {
			return errPacket(mysqlErrWrongValue, "22007", fmt.Sprintf("Incorrect datetime value: '%s' for column '%s' at row %d", text, column.Name, rowNumber))
		}
	}
	return nil
}

func stringLikeValue(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		return "", false
	}
}

func mysqlCharacterColumnLength(columnType string) (int, bool) {
	upper := strings.ToUpper(strings.TrimSpace(columnType))
	if !strings.HasPrefix(upper, "VARCHAR(") && !strings.HasPrefix(upper, "CHAR(") {
		return 0, false
	}
	open := strings.IndexByte(upper, '(')
	close := strings.IndexByte(upper[open+1:], ')')
	if open < 0 || close < 0 {
		return 0, false
	}
	length, err := parseInt64String(upper[open+1 : open+1+close])
	if err != nil || length < 0 {
		return 0, false
	}
	return int(length), true
}

func isMySQLIntegerType(columnType string) bool {
	upper := strings.ToUpper(strings.TrimSpace(columnType))
	return strings.Contains(upper, "INT")
}

func isMySQLDateTimeType(columnType string) bool {
	upper := strings.ToUpper(strings.TrimSpace(columnType))
	return strings.Contains(upper, "DATE") || strings.Contains(upper, "TIME")
}

func isMySQLDateTimeLiteral(value string) bool {
	if value == "" {
		return false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"15:04:05",
	} {
		if _, err := time.Parse(layout, value); err == nil {
			return true
		}
	}
	return false
}
