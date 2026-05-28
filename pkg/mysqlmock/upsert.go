package mysqlmock

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type mysqlUpsertStatement struct {
	TableName  string
	Columns    []string
	Rows       []mysqlUpsertRow
	UpdateSQL  string
	UpdateArgs []any
}

type mysqlUpsertRow struct {
	Exprs []string
	Args  []any
}

type mysqlUniqueKey struct {
	Name    string
	Columns []string
}

type sqliteTableColumn struct {
	Name       string
	Type       string
	NotNull    bool
	DefaultSQL string
	HasDefault bool
	PK         int
}

func (c *mysqlConn) execMySQLUpsert(ctx context.Context, sqlText string, args ...any) (okResult, bool, error) {
	stmt, ok, err := parseMySQLUpsertStatement(sqlText, args)
	if err != nil || !ok {
		return okResult{}, ok, err
	}
	if len(stmt.Columns) == 0 {
		columns, err := c.insertColumns(ctx, stmt.TableName)
		if err != nil {
			return okResult{}, true, err
		}
		stmt.Columns = columns
	}
	tableColumns, err := c.tableColumns(ctx, stmt.TableName)
	if err != nil {
		return okResult{}, true, err
	}

	var affected uint64
	var lastInsertID uint64
	for _, row := range stmt.Rows {
		values, err := c.evaluateUpsertRow(ctx, stmt.TableName, stmt.Columns, tableColumns, row)
		if err != nil {
			return okResult{}, true, err
		}
		result, err := c.execSQLite(ctx, buildSQLiteInsert(stmt.TableName, stmt.Columns), values...)
		if err == nil {
			affected++
			if result.LastInsertID != 0 && lastInsertID == 0 {
				lastInsertID = result.LastInsertID
			}
			continue
		}
		if !isSQLiteUniqueConstraint(err) {
			return okResult{}, true, err
		}

		rowID, err := c.findConflictRowID(ctx, stmt.TableName, stmt.Columns, values)
		if err != nil {
			return okResult{}, true, err
		}
		before, err := c.fetchRowByRowID(ctx, stmt.TableName, rowID)
		if err != nil {
			return okResult{}, true, err
		}
		updateSQL, updateArgs, err := buildSQLiteUpsertUpdate(stmt.TableName, stmt.Columns, values, stmt.UpdateSQL, stmt.UpdateArgs, rowID)
		if err != nil {
			return okResult{}, true, err
		}
		if _, err := c.execSQLite(ctx, updateSQL, updateArgs...); err != nil {
			return okResult{}, true, err
		}
		after, err := c.fetchRowByRowID(ctx, stmt.TableName, rowID)
		if err != nil {
			return okResult{}, true, err
		}
		if !reflect.DeepEqual(before, after) {
			affected += 2
		}
		if id, ok := c.autoIncrementValue(ctx, stmt.TableName, rowID); ok && id != 0 && lastInsertID == 0 {
			lastInsertID = id
		}
	}

	return okResult{AffectedRows: affected, LastInsertID: lastInsertID}, true, nil
}

func parseMySQLUpsertStatement(sqlText string, args []any) (mysqlUpsertStatement, bool, error) {
	insertSQL, updateSQL, ok := splitOnDuplicateKeyUpdate(sqlText)
	if !ok {
		return mysqlUpsertStatement{}, false, nil
	}

	pos := skipSQLSpaces(insertSQL, 0)
	if !consumeKeyword(insertSQL, &pos, "INSERT") {
		return mysqlUpsertStatement{}, false, nil
	}
	for {
		start := pos
		if !consumeKeyword(insertSQL, &pos, "LOW_PRIORITY") &&
			!consumeKeyword(insertSQL, &pos, "DELAYED") &&
			!consumeKeyword(insertSQL, &pos, "HIGH_PRIORITY") &&
			!consumeKeyword(insertSQL, &pos, "IGNORE") {
			pos = start
			break
		}
	}
	if !consumeKeyword(insertSQL, &pos, "INTO") {
		return mysqlUpsertStatement{}, false, nil
	}
	tableName, pos, ok := readSQLNameToken(insertSQL, pos)
	if !ok {
		return mysqlUpsertStatement{}, false, sqlCompatErrorf("Unsupported ON DUPLICATE KEY UPDATE insert target")
	}

	columns := []string{}
	next := skipSQLSpaces(insertSQL, pos)
	if next < len(insertSQL) && insertSQL[next] == '(' {
		columnsEnd, ok := parenthesizedSQLSpan(insertSQL, next)
		if !ok {
			return mysqlUpsertStatement{}, false, sqlCompatErrorf("Malformed ON DUPLICATE KEY UPDATE column list")
		}
		for _, item := range splitSQLTopLevelList(insertSQL[next+1 : columnsEnd-1]) {
			name := strings.TrimSpace(item)
			if name == "" {
				continue
			}
			columns = append(columns, unquoteSQLWord(name))
		}
		pos = columnsEnd
	}

	if !consumeKeyword(insertSQL, &pos, "VALUES") && !consumeKeyword(insertSQL, &pos, "VALUE") {
		return mysqlUpsertStatement{}, false, sqlCompatErrorf("Unsupported ON DUPLICATE KEY UPDATE insert form")
	}
	rows := []mysqlUpsertRow{}
	argPos := 0
	for {
		pos = skipSQLSpaces(insertSQL, pos)
		if pos >= len(insertSQL) {
			break
		}
		rowEnd, ok := parenthesizedSQLSpan(insertSQL, pos)
		if !ok {
			return mysqlUpsertStatement{}, false, sqlCompatErrorf("Malformed ON DUPLICATE KEY UPDATE values row")
		}
		exprs := splitSQLTopLevelList(insertSQL[pos+1 : rowEnd-1])
		placeholderCount := 0
		for _, expr := range exprs {
			placeholderCount += countPlaceholders(expr)
		}
		if argPos+placeholderCount > len(args) {
			return mysqlUpsertStatement{}, false, sqlCompatErrorf("ON DUPLICATE KEY UPDATE has too few arguments")
		}
		rowArgs := append([]any(nil), args[argPos:argPos+placeholderCount]...)
		argPos += placeholderCount
		rows = append(rows, mysqlUpsertRow{Exprs: exprs, Args: rowArgs})

		pos = skipSQLSpaces(insertSQL, rowEnd)
		if pos < len(insertSQL) && insertSQL[pos] == ',' {
			pos++
			continue
		}
		if pos != len(insertSQL) {
			return mysqlUpsertStatement{}, false, sqlCompatErrorf("Unsupported ON DUPLICATE KEY UPDATE values suffix")
		}
		break
	}
	updateArgs := append([]any(nil), args[argPos:]...)
	if countPlaceholders(updateSQL) != len(updateArgs) {
		return mysqlUpsertStatement{}, false, sqlCompatErrorf("ON DUPLICATE KEY UPDATE argument count mismatch")
	}

	return mysqlUpsertStatement{
		TableName:  tableName,
		Columns:    columns,
		Rows:       rows,
		UpdateSQL:  updateSQL,
		UpdateArgs: updateArgs,
	}, true, nil
}

func splitOnDuplicateKeyUpdate(sqlText string) (string, string, bool) {
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
		}
		if depth == 0 {
			pos := i
			if consumeKeyword(sqlText, &pos, "ON") &&
				consumeKeyword(sqlText, &pos, "DUPLICATE") &&
				consumeKeyword(sqlText, &pos, "KEY") &&
				consumeKeyword(sqlText, &pos, "UPDATE") {
				updateSQL := strings.TrimSpace(sqlText[pos:])
				updateSQL = strings.TrimSuffix(updateSQL, ";")
				return strings.TrimSpace(sqlText[:i]), updateSQL, true
			}
		}
		i++
	}
	return "", "", false
}

func (c *mysqlConn) evaluateUpsertRow(ctx context.Context, tableName string, columns []string, tableColumns []sqliteTableColumn, row mysqlUpsertRow) ([]any, error) {
	if len(row.Exprs) != len(columns) {
		return nil, sqlCompatErrorf("ON DUPLICATE KEY UPDATE values row has %d values for %d columns", len(row.Exprs), len(columns))
	}

	selectExprs := make([]string, len(row.Exprs))
	args := []any{}
	rowArgPos := 0
	for i, expr := range row.Exprs {
		if isBareDefaultExpr(expr) {
			value, err := c.defaultColumnValue(ctx, tableName, columns[i], tableColumns)
			if err != nil {
				return nil, err
			}
			selectExprs[i] = "?"
			args = append(args, value)
			continue
		}

		selectExprs[i] = expr
		placeholderCount := countPlaceholders(expr)
		if rowArgPos+placeholderCount > len(row.Args) {
			return nil, sqlCompatErrorf("ON DUPLICATE KEY UPDATE values row has too few arguments")
		}
		args = append(args, row.Args[rowArgPos:rowArgPos+placeholderCount]...)
		rowArgPos += placeholderCount
	}
	if rowArgPos != len(row.Args) {
		return nil, sqlCompatErrorf("ON DUPLICATE KEY UPDATE values row has unused arguments")
	}

	query := "SELECT " + strings.Join(selectExprs, ", ")
	rs, err := c.querySQLite(ctx, translateSQL(query), args...)
	if err != nil {
		return nil, err
	}
	if len(rs.Rows) != 1 {
		return nil, sqlCompatErrorf("ON DUPLICATE KEY UPDATE values row did not evaluate to one row")
	}
	return rs.Rows[0], nil
}

func isBareDefaultExpr(expr string) bool {
	pos := skipSQLSpaces(expr, 0)
	word, next, ok := readSQLIdentifier(expr, pos)
	if !ok || !strings.EqualFold(word, "DEFAULT") {
		return false
	}
	next = skipSQLSpaces(expr, next)
	return next == len(expr)
}

func (c *mysqlConn) defaultColumnValue(ctx context.Context, tableName, columnName string, tableColumns []sqliteTableColumn) (any, error) {
	column, ok := findSQLiteTableColumn(tableColumns, columnName)
	if !ok {
		return nil, sqlCompatErrorf("ON DUPLICATE KEY UPDATE references unknown DEFAULT column: %s", columnName)
	}
	if column.HasDefault {
		rs, err := c.querySQLite(ctx, "SELECT "+translateSQL(column.DefaultSQL))
		if err != nil {
			return nil, err
		}
		if len(rs.Rows) != 1 || len(rs.Rows[0]) != 1 {
			return nil, sqlCompatErrorf("default value for %s.%s did not evaluate to one value", tableName, columnName)
		}
		return rs.Rows[0][0], nil
	}
	if column.PK == 1 && strings.Contains(strings.ToUpper(column.Type), "INT") {
		return nil, nil
	}
	if !column.NotNull {
		return nil, nil
	}
	return nil, errPacket(mysqlErrUnknown, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", unquoteSQLWord(columnName)))
}

func findSQLiteTableColumn(columns []sqliteTableColumn, name string) (sqliteTableColumn, bool) {
	name = strings.ToLower(unquoteSQLWord(name))
	for _, column := range columns {
		if strings.EqualFold(column.Name, name) {
			return column, true
		}
	}
	return sqliteTableColumn{}, false
}

func buildSQLiteInsert(tableName string, columns []string) string {
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quoteIdent(unquoteSQLWord(tableName)), joinQuoted(columns), strings.Join(placeholders, ", "))
}

func buildSQLiteUpsertUpdate(tableName string, columns []string, values []any, updateSQL string, updateArgs []any, rowID int64) (string, []any, error) {
	valueByColumn := map[string]any{}
	for i, column := range columns {
		if i < len(values) {
			valueByColumn[strings.ToLower(unquoteSQLWord(column))] = values[i]
		}
	}

	var out strings.Builder
	args := []any{}
	updateArgPos := 0
	for i := 0; i < len(updateSQL); {
		if end, ok := quotedSQLSpan(updateSQL, i); ok {
			out.WriteString(updateSQL[i:end])
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(updateSQL, i); ok {
			out.WriteString(updateSQL[i:end])
			i = end
			continue
		}
		if updateSQL[i] == '?' {
			if updateArgPos >= len(updateArgs) {
				return "", nil, sqlCompatErrorf("ON DUPLICATE KEY UPDATE has too few update arguments")
			}
			out.WriteByte('?')
			args = append(args, updateArgs[updateArgPos])
			updateArgPos++
			i++
			continue
		}
		ident, end, ok := readSQLIdentifier(updateSQL, i)
		if ok && strings.EqualFold(ident, "VALUES") {
			callStart := skipSQLSpaces(updateSQL, end)
			callEnd, ok := parenthesizedSQLSpan(updateSQL, callStart)
			if ok {
				column := strings.TrimSpace(updateSQL[callStart+1 : callEnd-1])
				value, ok := valueByColumn[strings.ToLower(unquoteSQLWord(column))]
				if !ok {
					return "", nil, sqlCompatErrorf("ON DUPLICATE KEY UPDATE references unknown VALUES column: %s", column)
				}
				out.WriteByte('?')
				args = append(args, value)
				i = callEnd
				continue
			}
		}
		if ok {
			out.WriteString(updateSQL[i:end])
			i = end
			continue
		}
		out.WriteByte(updateSQL[i])
		i++
	}
	if updateArgPos != len(updateArgs) {
		return "", nil, sqlCompatErrorf("ON DUPLICATE KEY UPDATE has unused update arguments")
	}
	args = append(args, rowID)
	return fmt.Sprintf("UPDATE %s SET %s WHERE rowid = ?", quoteIdent(unquoteSQLWord(tableName)), translateSQL(out.String())), args, nil
}

func (c *mysqlConn) findConflictRowID(ctx context.Context, tableName string, columns []string, values []any) (int64, error) {
	keys, err := c.uniqueKeys(ctx, tableName)
	if err != nil {
		return 0, err
	}
	valueByColumn := map[string]any{}
	for i, column := range columns {
		if i < len(values) {
			valueByColumn[strings.ToLower(unquoteSQLWord(column))] = values[i]
		}
	}
	for _, key := range keys {
		where := []string{}
		args := []any{}
		skip := false
		for _, column := range key.Columns {
			value, ok := valueByColumn[strings.ToLower(column)]
			if !ok || value == nil {
				skip = true
				break
			}
			where = append(where, quoteIdent(column)+" = ?")
			args = append(args, value)
		}
		if skip || len(where) == 0 {
			continue
		}
		query := fmt.Sprintf("SELECT rowid FROM %s WHERE %s LIMIT 1", quoteIdent(unquoteSQLWord(tableName)), strings.Join(where, " AND "))
		rs, err := c.querySQLite(ctx, query, args...)
		if err != nil {
			return 0, err
		}
		if len(rs.Rows) == 0 {
			continue
		}
		return int64Value(rs.Rows[0][0])
	}
	return 0, sqlCompatErrorf("ON DUPLICATE KEY UPDATE conflict row was not found")
}

func (c *mysqlConn) uniqueKeys(ctx context.Context, tableName string) ([]mysqlUniqueKey, error) {
	columns, err := c.tableColumns(ctx, tableName)
	if err != nil {
		return nil, err
	}
	pkColumns := []sqliteTableColumn{}
	for _, column := range columns {
		if column.PK > 0 {
			pkColumns = append(pkColumns, column)
		}
	}
	sort.Slice(pkColumns, func(i, j int) bool {
		return pkColumns[i].PK < pkColumns[j].PK
	})

	keys := []mysqlUniqueKey{}
	if len(pkColumns) > 0 {
		key := mysqlUniqueKey{Name: "PRIMARY", Columns: make([]string, len(pkColumns))}
		for i, column := range pkColumns {
			key.Columns[i] = column.Name
		}
		keys = append(keys, key)
	}

	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.index_list("+quoteIdent(unquoteSQLWord(tableName))+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, err
		}
		if unique == 0 {
			continue
		}
		keyColumns, err := c.indexColumns(ctx, name)
		if err != nil {
			return nil, err
		}
		if len(keyColumns) > 0 {
			keys = append(keys, mysqlUniqueKey{Name: name, Columns: keyColumns})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

func (c *mysqlConn) indexColumns(ctx context.Context, indexName string) ([]string, error) {
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.index_info("+quoteIdent(indexName)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := []string{}
	for rows.Next() {
		var seqno int
		var cid int
		var name string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func (c *mysqlConn) tableColumns(ctx context.Context, tableName string) ([]sqliteTableColumn, error) {
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.table_info("+quoteIdent(unquoteSQLWord(tableName))+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := []sqliteTableColumn{}
	for rows.Next() {
		var cid int
		var column sqliteTableColumn
		var notNull int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &column.Name, &column.Type, &notNull, &defaultValue, &column.PK); err != nil {
			return nil, err
		}
		column.NotNull = notNull != 0
		if defaultValue.Valid {
			column.DefaultSQL = defaultValue.String
			column.HasDefault = true
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func (c *mysqlConn) insertColumns(ctx context.Context, tableName string) ([]string, error) {
	tableColumns, err := c.tableColumns(ctx, tableName)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(tableColumns))
	for i, column := range tableColumns {
		columns[i] = column.Name
	}
	return columns, nil
}

func (c *mysqlConn) fetchRowByRowID(ctx context.Context, tableName string, rowID int64) ([]any, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE rowid = ?", quoteIdent(unquoteSQLWord(tableName)))
	rs, err := c.querySQLite(ctx, query, rowID)
	if err != nil {
		return nil, err
	}
	if len(rs.Rows) != 1 {
		return nil, sqlCompatErrorf("rowid %d was not found", rowID)
	}
	return rs.Rows[0], nil
}

func (c *mysqlConn) autoIncrementValue(ctx context.Context, tableName string, rowID int64) (uint64, bool) {
	columns, err := c.tableColumns(ctx, tableName)
	if err != nil {
		return 0, false
	}
	for _, column := range columns {
		if column.PK != 1 || !strings.Contains(strings.ToUpper(column.Type), "INT") {
			continue
		}
		query := fmt.Sprintf("SELECT %s FROM %s WHERE rowid = ?", quoteIdent(column.Name), quoteIdent(unquoteSQLWord(tableName)))
		rs, err := c.querySQLite(ctx, query, rowID)
		if err != nil || len(rs.Rows) != 1 || len(rs.Rows[0]) != 1 {
			return 0, false
		}
		id, err := int64Value(rs.Rows[0][0])
		if err != nil || id < 0 {
			return 0, false
		}
		return uint64(id), true
	}
	return 0, false
}

func int64Value(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case uint64:
		if v <= uint64(^uint64(0)>>1) {
			return int64(v), nil
		}
	case []byte:
		return parseInt64String(string(v))
	case string:
		return parseInt64String(v)
	}
	return 0, fmt.Errorf("cannot convert %T to int64", value)
}

func parseInt64String(value string) (int64, error) {
	var out int64
	if _, err := fmt.Sscan(value, &out); err != nil {
		return 0, err
	}
	return out, nil
}

func isSQLiteUniqueConstraint(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "unique constraint")
}
