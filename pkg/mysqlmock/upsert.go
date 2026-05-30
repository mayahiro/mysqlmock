package mysqlmock

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

type mysqlUpsertStatement struct {
	TableName  string
	Columns    []string
	Rows       []mysqlUpsertRow
	ValueAlias string
	UpdateSQL  string
	UpdateArgs []any
}

type mysqlUpsertStatementTemplate struct {
	TableName              string
	Columns                []string
	Rows                   []mysqlUpsertRowTemplate
	ValueAlias             string
	UpdateSQL              string
	UpdatePlaceholderCount int
}

type mysqlUpsertRow struct {
	Exprs []string
	Args  []any
}

type mysqlUpsertRowTemplate struct {
	Exprs            []string
	PlaceholderCount int
}

type mysqlInsertStatement struct {
	TableName string
	Columns   []string
	Rows      []mysqlUpsertRow
	Ignore    bool
	Replace   bool
}

type mysqlInsertStatementTemplate struct {
	TableName string
	Columns   []string
	Rows      []mysqlUpsertRowTemplate
	Ignore    bool
	Replace   bool
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

type cachedTableColumns struct {
	SchemaVersion uint64
	Columns       []sqliteTableColumn
}

type cachedUniqueKeys struct {
	SchemaVersion uint64
	Keys          []mysqlUniqueKey
}

func (c *mysqlConn) execMySQLUpsert(ctx context.Context, sqlText string, args ...any) (okResult, bool, error) {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("mysql.upsert_compat", time.Since(start))
	}()

	stmt, ok, err := c.server.parseMySQLUpsertStatementCached(sqlText, args)
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
		insertColumns, insertValues := c.applyImplicitDefaultInsertValues(tableColumns, stmt.Columns, values)
		autoIncrement, err := c.applyMySQLAutoIncrementInsertValue(ctx, stmt.TableName, tableColumns, &insertColumns, &insertValues)
		if err != nil {
			return okResult{}, true, err
		}
		result, err := c.execSQLite(ctx, buildSQLiteInsert(stmt.TableName, insertColumns), insertValues...)
		if err == nil {
			affected++
			if autoIncrement.Value != 0 {
				c.server.recordMySQLAutoIncrementAllocation(stmt.TableName, autoIncrement.Value)
			}
			if autoIncrement.Generated && autoIncrement.Value != 0 && lastInsertID == 0 {
				lastInsertID = autoIncrement.Value
			} else if result.LastInsertID != 0 && lastInsertID == 0 {
				lastInsertID = result.LastInsertID
			}
			continue
		}
		if !isSQLiteUniqueConstraint(err) {
			return okResult{}, true, err
		}

		rowID, err := c.findConflictRowID(ctx, stmt.TableName, insertColumns, insertValues)
		if err != nil {
			return okResult{}, true, err
		}
		before, err := c.fetchRowByRowID(ctx, stmt.TableName, rowID)
		if err != nil {
			return okResult{}, true, err
		}
		updateSQL, updateArgs, err := buildSQLiteUpsertUpdate(stmt.TableName, insertColumns, insertValues, stmt.ValueAlias, stmt.UpdateSQL, stmt.UpdateArgs, rowID)
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

func (c *mysqlConn) execMySQLInsertCompatibility(ctx context.Context, sqlText string, args ...any) (okResult, bool, error) {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("mysql.insert_compat", time.Since(start))
	}()

	stmt, ok, err := c.server.parseMySQLInsertStatementCached(sqlText, args)
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
	if !stmt.Ignore && !stmt.Replace {
		if c.server.cfg.Compat.ImplicitDefaults || insertStatementUsesDefaultValues(stmt) || c.needsMySQLAutoIncrementInsertCompatibility(tableColumns, stmt.TableName) {
			return c.execMySQLPlainInsert(ctx, stmt, tableColumns)
		}
		return okResult{}, false, nil
	}

	if stmt.Ignore {
		return c.execMySQLInsertIgnore(ctx, stmt, tableColumns)
	}
	return c.execMySQLReplace(ctx, stmt, tableColumns)
}

func (c *mysqlConn) execMySQLPlainInsert(ctx context.Context, stmt mysqlInsertStatement, tableColumns []sqliteTableColumn) (okResult, bool, error) {
	var affected uint64
	var lastInsertID uint64
	for _, row := range stmt.Rows {
		values, err := c.evaluateUpsertRow(ctx, stmt.TableName, stmt.Columns, tableColumns, row)
		if err != nil {
			return okResult{}, true, err
		}
		insertColumns, insertValues := c.applyImplicitDefaultInsertValues(tableColumns, stmt.Columns, values)
		autoIncrement, err := c.applyMySQLAutoIncrementInsertValue(ctx, stmt.TableName, tableColumns, &insertColumns, &insertValues)
		if err != nil {
			return okResult{}, true, err
		}
		result, err := c.execSQLite(ctx, buildSQLiteInsert(stmt.TableName, insertColumns), insertValues...)
		if err != nil {
			return okResult{}, true, err
		}
		affected++
		if autoIncrement.Value != 0 {
			c.server.recordMySQLAutoIncrementAllocation(stmt.TableName, autoIncrement.Value)
		}
		if autoIncrement.Generated && autoIncrement.Value != 0 && lastInsertID == 0 {
			lastInsertID = autoIncrement.Value
		} else if result.LastInsertID != 0 && lastInsertID == 0 {
			lastInsertID = result.LastInsertID
		}
	}
	return okResult{AffectedRows: affected, LastInsertID: lastInsertID}, true, nil
}

func (c *mysqlConn) execMySQLInsertIgnore(ctx context.Context, stmt mysqlInsertStatement, tableColumns []sqliteTableColumn) (okResult, bool, error) {
	var affected uint64
	var lastInsertID uint64
	var warnings uint16
	for _, row := range stmt.Rows {
		values, err := c.evaluateUpsertRow(ctx, stmt.TableName, stmt.Columns, tableColumns, row)
		if err != nil {
			return okResult{}, true, err
		}
		insertColumns, insertValues := c.applyImplicitDefaultInsertValues(tableColumns, stmt.Columns, values)
		autoIncrement, err := c.applyMySQLAutoIncrementInsertValue(ctx, stmt.TableName, tableColumns, &insertColumns, &insertValues)
		if err != nil {
			return okResult{}, true, err
		}
		result, err := c.execSQLite(ctx, buildSQLiteInsert(stmt.TableName, insertColumns), insertValues...)
		if err == nil {
			affected++
			if autoIncrement.Value != 0 {
				c.server.recordMySQLAutoIncrementAllocation(stmt.TableName, autoIncrement.Value)
			}
			if autoIncrement.Generated && autoIncrement.Value != 0 && lastInsertID == 0 {
				lastInsertID = autoIncrement.Value
			} else if result.LastInsertID != 0 && lastInsertID == 0 {
				lastInsertID = result.LastInsertID
			}
			continue
		}
		if !isSQLiteUniqueConstraint(err) {
			return okResult{}, true, err
		}
		warnings++
	}
	return okResult{AffectedRows: affected, LastInsertID: lastInsertID, Warnings: warnings}, true, nil
}

func (c *mysqlConn) execMySQLReplace(ctx context.Context, stmt mysqlInsertStatement, tableColumns []sqliteTableColumn) (okResult, bool, error) {
	var affected uint64
	var lastInsertID uint64
	for _, row := range stmt.Rows {
		values, err := c.evaluateUpsertRow(ctx, stmt.TableName, stmt.Columns, tableColumns, row)
		if err != nil {
			return okResult{}, true, err
		}
		insertColumns, insertValues := c.applyImplicitDefaultInsertValues(tableColumns, stmt.Columns, values)
		autoIncrement, err := c.applyMySQLAutoIncrementInsertValue(ctx, stmt.TableName, tableColumns, &insertColumns, &insertValues)
		if err != nil {
			return okResult{}, true, err
		}
		rowIDs, err := c.findConflictRowIDs(ctx, stmt.TableName, insertColumns, insertValues)
		if err != nil {
			return okResult{}, true, err
		}
		for _, rowID := range rowIDs {
			if _, err := c.execSQLite(ctx, fmt.Sprintf("DELETE FROM %s WHERE rowid = ?", quoteIdent(unquoteSQLWord(stmt.TableName))), rowID); err != nil {
				return okResult{}, true, err
			}
			affected++
		}
		result, err := c.execSQLite(ctx, buildSQLiteInsert(stmt.TableName, insertColumns), insertValues...)
		if err != nil {
			return okResult{}, true, err
		}
		affected++
		if autoIncrement.Value != 0 {
			c.server.recordMySQLAutoIncrementAllocation(stmt.TableName, autoIncrement.Value)
		}
		if autoIncrement.Generated && autoIncrement.Value != 0 && lastInsertID == 0 {
			lastInsertID = autoIncrement.Value
		} else if result.LastInsertID != 0 && lastInsertID == 0 {
			lastInsertID = result.LastInsertID
		}
	}
	return okResult{AffectedRows: affected, LastInsertID: lastInsertID}, true, nil
}

func parseMySQLUpsertStatementTemplate(sqlText string) (mysqlUpsertStatementTemplate, bool, error) {
	insertSQL, updateSQL, ok := splitOnDuplicateKeyUpdate(sqlText)
	if !ok {
		return mysqlUpsertStatementTemplate{}, false, nil
	}

	pos := skipSQLSpaces(insertSQL, 0)
	if !consumeKeyword(insertSQL, &pos, "INSERT") {
		return mysqlUpsertStatementTemplate{}, false, nil
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
		return mysqlUpsertStatementTemplate{}, false, nil
	}
	tableName, pos, ok := readSQLQualifiedName(insertSQL, pos)
	if !ok {
		return mysqlUpsertStatementTemplate{}, false, sqlCompatErrorf("Unsupported ON DUPLICATE KEY UPDATE insert target")
	}

	columns := []string{}
	next := skipSQLSpaces(insertSQL, pos)
	if next < len(insertSQL) && insertSQL[next] == '(' {
		columnsEnd, ok := parenthesizedSQLSpan(insertSQL, next)
		if !ok {
			return mysqlUpsertStatementTemplate{}, false, sqlCompatErrorf("Malformed ON DUPLICATE KEY UPDATE column list")
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
		return mysqlUpsertStatementTemplate{}, false, sqlCompatErrorf("Unsupported ON DUPLICATE KEY UPDATE insert form")
	}
	rows := []mysqlUpsertRowTemplate{}
	valueAlias := ""
	for {
		pos = skipSQLSpaces(insertSQL, pos)
		if pos >= len(insertSQL) {
			break
		}
		rowEnd, ok := parenthesizedSQLSpan(insertSQL, pos)
		if !ok {
			return mysqlUpsertStatementTemplate{}, false, sqlCompatErrorf("Malformed ON DUPLICATE KEY UPDATE values row")
		}
		exprs := splitSQLTopLevelList(insertSQL[pos+1 : rowEnd-1])
		placeholderCount := 0
		for _, expr := range exprs {
			placeholderCount += countPlaceholders(expr)
		}
		rows = append(rows, mysqlUpsertRowTemplate{Exprs: exprs, PlaceholderCount: placeholderCount})

		pos = skipSQLSpaces(insertSQL, rowEnd)
		if pos < len(insertSQL) && insertSQL[pos] == ',' {
			pos++
			continue
		}
		nextAlias, next, ok := consumeOptionalValuesAlias(insertSQL, pos)
		if ok {
			pos = next
			valueAlias = nextAlias
		}
		if pos != len(insertSQL) {
			return mysqlUpsertStatementTemplate{}, false, sqlCompatErrorf("Unsupported ON DUPLICATE KEY UPDATE values suffix")
		}
		break
	}

	return mysqlUpsertStatementTemplate{
		TableName:              tableName,
		Columns:                columns,
		Rows:                   rows,
		ValueAlias:             valueAlias,
		UpdateSQL:              updateSQL,
		UpdatePlaceholderCount: countPlaceholders(updateSQL),
	}, true, nil
}

func (tpl mysqlUpsertStatementTemplate) bind(args []any) (mysqlUpsertStatement, error) {
	rows := make([]mysqlUpsertRow, len(tpl.Rows))
	argPos := 0
	for i, row := range tpl.Rows {
		if argPos+row.PlaceholderCount > len(args) {
			return mysqlUpsertStatement{}, sqlCompatErrorf("ON DUPLICATE KEY UPDATE has too few arguments")
		}
		rows[i] = mysqlUpsertRow{
			Exprs: row.Exprs,
			Args:  args[argPos : argPos+row.PlaceholderCount],
		}
		argPos += row.PlaceholderCount
	}
	updateArgs := args[argPos:]
	if tpl.UpdatePlaceholderCount != len(updateArgs) {
		return mysqlUpsertStatement{}, sqlCompatErrorf("ON DUPLICATE KEY UPDATE argument count mismatch")
	}
	return mysqlUpsertStatement{
		TableName:  tpl.TableName,
		Columns:    append([]string(nil), tpl.Columns...),
		Rows:       rows,
		ValueAlias: tpl.ValueAlias,
		UpdateSQL:  tpl.UpdateSQL,
		UpdateArgs: updateArgs,
	}, nil
}

func consumeOptionalValuesAlias(sqlText string, pos int) (string, int, bool) {
	pos = skipSQLSpaces(sqlText, pos)
	if pos < len(sqlText) && sqlText[pos] == ';' {
		pos = skipSQLSpaces(sqlText, pos+1)
	}
	if pos >= len(sqlText) {
		return "", pos, true
	}
	start := pos
	if !consumeKeyword(sqlText, &pos, "AS") {
		return "", start, false
	}
	name, next, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return "", start, false
	}
	pos = skipSQLSpaces(sqlText, next)
	if end, ok := parenthesizedSQLSpan(sqlText, pos); ok {
		pos = skipSQLSpaces(sqlText, end)
	}
	if pos < len(sqlText) && sqlText[pos] == ';' {
		pos = skipSQLSpaces(sqlText, pos+1)
	}
	return unquoteSQLWord(name), pos, true
}

func parseMySQLInsertStatementTemplate(sqlText string) (mysqlInsertStatementTemplate, bool, error) {
	pos := skipSQLSpaces(sqlText, 0)
	replace := consumeKeyword(sqlText, &pos, "REPLACE")
	if !replace && !consumeKeyword(sqlText, &pos, "INSERT") {
		return mysqlInsertStatementTemplate{}, false, nil
	}

	ignore := false
	for {
		start := pos
		switch {
		case consumeKeyword(sqlText, &pos, "LOW_PRIORITY"):
		case consumeKeyword(sqlText, &pos, "DELAYED"):
		case consumeKeyword(sqlText, &pos, "HIGH_PRIORITY"):
		case consumeKeyword(sqlText, &pos, "IGNORE"):
			ignore = true
		default:
			pos = start
			goto modifiersDone
		}
	}

modifiersDone:
	if replace {
		_ = consumeKeyword(sqlText, &pos, "INTO")
	} else if !consumeKeyword(sqlText, &pos, "INTO") {
		return mysqlInsertStatementTemplate{}, false, nil
	}
	tableName, pos, ok := readSQLQualifiedName(sqlText, pos)
	if !ok {
		return mysqlInsertStatementTemplate{}, false, nil
	}

	columns := []string{}
	next := skipSQLSpaces(sqlText, pos)
	if next < len(sqlText) && sqlText[next] == '(' {
		columnsEnd, ok := parenthesizedSQLSpan(sqlText, next)
		if !ok {
			return mysqlInsertStatementTemplate{}, false, nil
		}
		for _, item := range splitSQLTopLevelList(sqlText[next+1 : columnsEnd-1]) {
			name := strings.TrimSpace(item)
			if name == "" {
				continue
			}
			columns = append(columns, unquoteSQLWord(name))
		}
		pos = columnsEnd
	}

	if !consumeKeyword(sqlText, &pos, "VALUES") && !consumeKeyword(sqlText, &pos, "VALUE") {
		return mysqlInsertStatementTemplate{}, false, nil
	}
	rows, ok := parseMySQLValueRowTemplates(sqlText, pos)
	if !ok {
		return mysqlInsertStatementTemplate{}, false, nil
	}
	return mysqlInsertStatementTemplate{
		TableName: tableName,
		Columns:   columns,
		Rows:      rows,
		Ignore:    ignore,
		Replace:   replace,
	}, true, nil
}

func (tpl mysqlInsertStatementTemplate) bind(args []any) (mysqlInsertStatement, bool) {
	rows := make([]mysqlUpsertRow, len(tpl.Rows))
	argPos := 0
	for i, row := range tpl.Rows {
		if argPos+row.PlaceholderCount > len(args) {
			return mysqlInsertStatement{}, false
		}
		rows[i] = mysqlUpsertRow{
			Exprs: row.Exprs,
			Args:  args[argPos : argPos+row.PlaceholderCount],
		}
		argPos += row.PlaceholderCount
	}
	if argPos != len(args) {
		return mysqlInsertStatement{}, false
	}
	return mysqlInsertStatement{
		TableName: tpl.TableName,
		Columns:   append([]string(nil), tpl.Columns...),
		Rows:      rows,
		Ignore:    tpl.Ignore,
		Replace:   tpl.Replace,
	}, true
}

func parseMySQLValueRowTemplates(sqlText string, pos int) ([]mysqlUpsertRowTemplate, bool) {
	rows := []mysqlUpsertRowTemplate{}
	for {
		pos = skipSQLSpaces(sqlText, pos)
		if pos >= len(sqlText) {
			break
		}
		rowEnd, ok := parenthesizedSQLSpan(sqlText, pos)
		if !ok {
			return nil, false
		}
		exprs := splitSQLTopLevelList(sqlText[pos+1 : rowEnd-1])
		placeholderCount := 0
		for _, expr := range exprs {
			placeholderCount += countPlaceholders(expr)
		}
		rows = append(rows, mysqlUpsertRowTemplate{Exprs: exprs, PlaceholderCount: placeholderCount})

		pos = skipSQLSpaces(sqlText, rowEnd)
		if pos < len(sqlText) && sqlText[pos] == ',' {
			pos++
			continue
		}
		if pos < len(sqlText) && sqlText[pos] == ';' {
			pos = skipSQLSpaces(sqlText, pos+1)
		}
		if pos != len(sqlText) {
			return nil, false
		}
		break
	}
	return rows, true
}

func parseMySQLValuesRows(sqlText string, pos int, args []any) ([]mysqlUpsertRow, int, bool) {
	rows := []mysqlUpsertRow{}
	argPos := 0
	for {
		pos = skipSQLSpaces(sqlText, pos)
		if pos >= len(sqlText) {
			break
		}
		rowEnd, ok := parenthesizedSQLSpan(sqlText, pos)
		if !ok {
			return nil, 0, false
		}
		exprs := splitSQLTopLevelList(sqlText[pos+1 : rowEnd-1])
		placeholderCount := 0
		for _, expr := range exprs {
			placeholderCount += countPlaceholders(expr)
		}
		if argPos+placeholderCount > len(args) {
			return nil, 0, false
		}
		rowArgs := append([]any(nil), args[argPos:argPos+placeholderCount]...)
		argPos += placeholderCount
		rows = append(rows, mysqlUpsertRow{Exprs: exprs, Args: rowArgs})

		pos = skipSQLSpaces(sqlText, rowEnd)
		if pos < len(sqlText) && sqlText[pos] == ',' {
			pos++
			continue
		}
		if pos < len(sqlText) && sqlText[pos] == ';' {
			pos = skipSQLSpaces(sqlText, pos+1)
		}
		if pos != len(sqlText) {
			return nil, 0, false
		}
		break
	}
	return rows, argPos, true
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
	if isPlaceholderOnlyUpsertRow(row) {
		return append([]any(nil), row.Args...), nil
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

func isPlaceholderOnlyUpsertRow(row mysqlUpsertRow) bool {
	if len(row.Args) != len(row.Exprs) {
		return false
	}
	for _, expr := range row.Exprs {
		if !isBarePlaceholderExpr(expr) {
			return false
		}
	}
	return true
}

func (c *mysqlConn) applyImplicitDefaultInsertValues(tableColumns []sqliteTableColumn, columns []string, values []any) ([]string, []any) {
	if !c.server.cfg.Compat.ImplicitDefaults {
		return columns, values
	}
	outColumns := append([]string(nil), columns...)
	outValues := append([]any(nil), values...)
	seen := make(map[string]int, len(columns))
	for i, column := range columns {
		key := strings.ToLower(unquoteSQLWord(column))
		seen[key] = i
		if i >= len(outValues) || outValues[i] != nil {
			continue
		}
		tableColumn, ok := findSQLiteTableColumn(tableColumns, column)
		if !ok || !needsMySQLImplicitDefault(tableColumn) {
			continue
		}
		if implicit, ok := mysqlImplicitDefaultValue(tableColumn); ok {
			outValues[i] = implicit
		}
	}
	for _, tableColumn := range tableColumns {
		key := strings.ToLower(tableColumn.Name)
		if _, ok := seen[key]; ok || !needsMySQLImplicitDefault(tableColumn) {
			continue
		}
		if implicit, ok := mysqlImplicitDefaultValue(tableColumn); ok {
			outColumns = append(outColumns, tableColumn.Name)
			outValues = append(outValues, implicit)
		}
	}
	return outColumns, outValues
}

func insertStatementUsesDefaultValues(stmt mysqlInsertStatement) bool {
	for _, row := range stmt.Rows {
		for _, expr := range row.Exprs {
			if isBareDefaultExpr(expr) {
				return true
			}
		}
	}
	return false
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
	if metadata, ok := c.server.lookupMySQLAutoIncrementColumn(tableName); ok && strings.EqualFold(metadata.ColumnName, unquoteSQLWord(columnName)) {
		return nil, nil
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
	if c.server.cfg.Compat.ImplicitDefaults {
		if implicit, ok := mysqlImplicitDefaultValue(column); ok {
			return implicit, nil
		}
	}
	return nil, errPacket(mysqlErrUnknown, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", unquoteSQLWord(columnName)))
}

func needsMySQLImplicitDefault(column sqliteTableColumn) bool {
	return column.NotNull && !column.HasDefault && column.PK == 0
}

func mysqlImplicitDefaultValue(column sqliteTableColumn) (any, bool) {
	upper := strings.ToUpper(strings.TrimSpace(column.Type))
	switch {
	case strings.Contains(upper, "INT"), strings.Contains(upper, "BOOL"), strings.Contains(upper, "BIT"):
		return int64(0), true
	case strings.Contains(upper, "DECIMAL"), strings.Contains(upper, "NUMERIC"), strings.Contains(upper, "REAL"),
		strings.Contains(upper, "DOUBLE"), strings.Contains(upper, "FLOAT"):
		return float64(0), true
	case strings.Contains(upper, "DATETIME"), strings.Contains(upper, "TIMESTAMP"):
		return "0000-00-00 00:00:00", true
	case strings.Contains(upper, "DATE"):
		return "0000-00-00", true
	case strings.Contains(upper, "TIME"):
		return "00:00:00", true
	case strings.Contains(upper, "BLOB"):
		return []byte{}, true
	default:
		return "", true
	}
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

func buildSQLiteUpsertUpdate(tableName string, columns []string, values []any, valueAlias, updateSQL string, updateArgs []any, rowID int64) (string, []any, error) {
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
		if updateSQL[i] == '\'' {
			if end, ok := quotedSQLSpan(updateSQL, i); ok {
				out.WriteString(updateSQL[i:end])
				i = end
				continue
			}
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
		replacement, next, replacementArgs, ok, err := rewriteUpsertReference(updateSQL, i, tableName, valueAlias, valueByColumn)
		if err != nil {
			return "", nil, err
		}
		if ok {
			out.WriteString(replacement)
			args = append(args, replacementArgs...)
			i = next
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

func rewriteUpsertReference(updateSQL string, pos int, tableName, valueAlias string, valueByColumn map[string]any) (string, int, []any, bool, error) {
	name, end, ok := readSQLNameToken(updateSQL, pos)
	if !ok {
		return "", pos, nil, false, nil
	}
	unquotedName := unquoteSQLWord(name)
	if strings.EqualFold(unquotedName, "VALUES") {
		callStart := skipSQLSpaces(updateSQL, end)
		callEnd, ok := parenthesizedSQLSpan(updateSQL, callStart)
		if !ok {
			return updateSQL[pos:end], end, nil, true, nil
		}
		column := strings.TrimSpace(updateSQL[callStart+1 : callEnd-1])
		value, ok := valueByColumn[strings.ToLower(unquoteSQLWord(column))]
		if !ok {
			return "", pos, nil, false, sqlCompatErrorf("ON DUPLICATE KEY UPDATE references unknown VALUES column: %s", column)
		}
		return "?", callEnd, []any{value}, true, nil
	}

	dot := skipSQLSpaces(updateSQL, end)
	if dot >= len(updateSQL) || updateSQL[dot] != '.' {
		return updateSQL[pos:end], end, nil, true, nil
	}
	column, columnEnd, ok := readSQLNameToken(updateSQL, dot+1)
	if !ok {
		return updateSQL[pos:end], end, nil, true, nil
	}
	unquotedColumn := unquoteSQLWord(column)
	switch {
	case valueAlias != "" && strings.EqualFold(unquotedName, valueAlias):
		value, ok := valueByColumn[strings.ToLower(unquotedColumn)]
		if !ok {
			return "", pos, nil, false, sqlCompatErrorf("ON DUPLICATE KEY UPDATE references unknown alias column: %s.%s", unquotedName, unquotedColumn)
		}
		return "?", columnEnd, []any{value}, true, nil
	case strings.EqualFold(unquotedName, unquoteSQLWord(tableName)):
		return quoteIdent(unquotedColumn), columnEnd, nil, true, nil
	default:
		return updateSQL[pos:columnEnd], columnEnd, nil, true, nil
	}
}

func (c *mysqlConn) findConflictRowID(ctx context.Context, tableName string, columns []string, values []any) (int64, error) {
	rowIDs, err := c.findConflictRowIDs(ctx, tableName, columns, values)
	if err != nil {
		return 0, err
	}
	if len(rowIDs) > 0 {
		return rowIDs[0], nil
	}
	return 0, sqlCompatErrorf("ON DUPLICATE KEY UPDATE conflict row was not found")
}

func (c *mysqlConn) findConflictRowIDs(ctx context.Context, tableName string, columns []string, values []any) ([]int64, error) {
	keys, err := c.uniqueKeys(ctx, tableName)
	if err != nil {
		return nil, err
	}
	valueByColumn := map[string]any{}
	for i, column := range columns {
		if i < len(values) {
			valueByColumn[strings.ToLower(unquoteSQLWord(column))] = values[i]
		}
	}
	seen := map[int64]bool{}
	rowIDs := []int64{}
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
		query := fmt.Sprintf("SELECT rowid FROM %s WHERE %s", quoteIdent(unquoteSQLWord(tableName)), strings.Join(where, " AND "))
		rs, err := c.querySQLite(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for _, row := range rs.Rows {
			if len(row) == 0 {
				continue
			}
			rowID, err := int64Value(row[0])
			if err != nil {
				return nil, err
			}
			if !seen[rowID] {
				seen[rowID] = true
				rowIDs = append(rowIDs, rowID)
			}
		}
	}
	return rowIDs, nil
}

func (c *mysqlConn) uniqueKeys(ctx context.Context, tableName string) ([]mysqlUniqueKey, error) {
	version := c.server.currentSchemaVersion()
	cacheKey := normalizedTableCacheKey(tableName)
	c.server.metadataMu.Lock()
	if cached, ok := c.server.uniqueKeys[cacheKey]; ok && cached.SchemaVersion == version {
		keys := cloneMySQLUniqueKeys(cached.Keys)
		c.server.metadataMu.Unlock()
		return keys, nil
	}
	c.server.metadataMu.Unlock()

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
	c.server.metadataMu.Lock()
	if c.server.uniqueKeys == nil {
		c.server.uniqueKeys = map[string]cachedUniqueKeys{}
	}
	c.server.uniqueKeys[cacheKey] = cachedUniqueKeys{
		SchemaVersion: version,
		Keys:          cloneMySQLUniqueKeys(keys),
	}
	c.server.metadataMu.Unlock()
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
		var name sql.NullString
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		if name.Valid {
			columns = append(columns, name.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func (c *mysqlConn) tableColumns(ctx context.Context, tableName string) ([]sqliteTableColumn, error) {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("metadata.table_columns", time.Since(start))
	}()

	version := c.server.currentSchemaVersion()
	cacheKey := normalizedTableCacheKey(tableName)
	c.server.metadataMu.Lock()
	if cached, ok := c.server.tableColumns[cacheKey]; ok && cached.SchemaVersion == version {
		columns := cloneSQLiteTableColumns(cached.Columns)
		c.server.metadataMu.Unlock()
		return columns, nil
	}
	c.server.metadataMu.Unlock()

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
	c.server.metadataMu.Lock()
	if c.server.tableColumns == nil {
		c.server.tableColumns = map[string]cachedTableColumns{}
	}
	c.server.tableColumns[cacheKey] = cachedTableColumns{
		SchemaVersion: version,
		Columns:       cloneSQLiteTableColumns(columns),
	}
	c.server.metadataMu.Unlock()
	return columns, nil
}

func normalizedTableCacheKey(tableName string) string {
	return strings.ToLower(unquoteSQLWord(tableName))
}

func cloneSQLiteTableColumns(columns []sqliteTableColumn) []sqliteTableColumn {
	out := make([]sqliteTableColumn, len(columns))
	copy(out, columns)
	return out
}

func cloneMySQLUniqueKeys(keys []mysqlUniqueKey) []mysqlUniqueKey {
	out := make([]mysqlUniqueKey, len(keys))
	for i, key := range keys {
		out[i] = mysqlUniqueKey{
			Name:    key.Name,
			Columns: append([]string(nil), key.Columns...),
		}
	}
	return out
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
