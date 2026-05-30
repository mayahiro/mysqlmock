package mysqlmock

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

func isAdvisoryLockQuery(upperSQL string) bool {
	return strings.HasPrefix(upperSQL, "SELECT GET_LOCK(") ||
		strings.HasPrefix(upperSQL, "SELECT RELEASE_LOCK(")
}

func (c *mysqlConn) advisoryLockResult(sqlText string) resultSet {
	expr := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(sqlText), "SELECT"))
	expr = strings.TrimSuffix(expr, ";")
	name, ok := advisoryLockName(expr)
	if !ok {
		return oneRow(expr, nil)
	}
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(expr)), "RELEASE_LOCK") {
		return oneRow(expr, c.server.releaseAdvisoryLock(name, c.connectionID))
	}
	return oneRow(expr, c.server.getAdvisoryLock(name, c.connectionID))
}

func isShowFullFieldsQuery(upperSQL string) bool {
	return strings.HasPrefix(upperSQL, "SHOW FULL FIELDS FROM ") ||
		strings.HasPrefix(upperSQL, "SHOW FULL COLUMNS FROM ") ||
		strings.HasPrefix(upperSQL, "SHOW FIELDS FROM ") ||
		strings.HasPrefix(upperSQL, "SHOW COLUMNS FROM ")
}

func isShowCreateTableQuery(upperSQL string) bool {
	return strings.HasPrefix(upperSQL, "SHOW CREATE TABLE ")
}

func isShowKeysQuery(upperSQL string) bool {
	return strings.HasPrefix(upperSQL, "SHOW KEYS FROM ") ||
		strings.HasPrefix(upperSQL, "SHOW INDEX FROM ") ||
		strings.HasPrefix(upperSQL, "SHOW INDEXES FROM ")
}

func (c *mysqlConn) showFullFields(ctx context.Context, sqlText string) (resultSet, error) {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("information_schema.show_full_fields", time.Since(start))
	}()

	c.server.stats.recordShowFullFieldsQuery()
	tableName, likePattern, ok := parseShowFullFields(sqlText)
	if !ok {
		return resultSet{}, c.server.unsupportedError(sqlText)
	}
	version := c.server.currentSchemaVersion()
	if cached, ok := c.server.cachedShowFullFieldsResult(c.currentDB, tableName, likePattern, c.collationConnection, version); ok {
		return cached, nil
	}
	result, err := c.showFullFieldsDirect(ctx, tableName, likePattern)
	if err != nil {
		return resultSet{}, err
	}
	c.server.markShowFullFieldsResult(c.currentDB, tableName, likePattern, c.collationConnection, version, result)
	return result, nil
}

type showFullFieldColumn struct {
	name       string
	dataType   string
	columnType string
	isNullable string
	columnKey  string
	defaultVal any
	extra      string
}

func (c *mysqlConn) showFullFieldsDirect(ctx context.Context, tableName, likePattern string) (resultSet, error) {
	var createSQL sql.NullString
	err := c.sqliteConn.QueryRowContext(ctx, `
SELECT sql
FROM main.sqlite_master
WHERE type IN ('table', 'view')
  AND name = ?
  AND name NOT LIKE 'sqlite_%'`, tableName).Scan(&createSQL)
	if err != nil {
		if err == sql.ErrNoRows {
			return resultSet{}, errPacket(mysqlErrNoSuchTable, "42S02", "Table '"+c.currentDB+"."+tableName+"' doesn't exist")
		}
		return resultSet{}, err
	}
	columns, err := c.showFullFieldColumns(ctx, tableName, createSQL.String)
	if err != nil {
		return resultSet{}, err
	}
	keys, err := c.showFullFieldIndexKeys(ctx, tableName)
	if err != nil {
		return resultSet{}, err
	}

	rows := make([][]any, 0, len(columns))
	for _, column := range columns {
		if likePattern != "" && !sqlLikeMatch(column.name, likePattern) {
			continue
		}
		collation := any(nil)
		if columnUsesTextCollation(column.dataType, column.columnType) {
			collation = c.collationConnection
		}
		columnKey := column.columnKey
		if key, ok := keys[showFullFieldColumnKey(column.name)]; ok && columnKey != "PRI" {
			columnKey = key
		}
		rows = append(rows, []any{
			column.name,
			column.columnType,
			collation,
			column.isNullable,
			columnKey,
			column.defaultVal,
			column.extra,
			"select,insert,update,references",
			"",
		})
	}
	return resultSet{
		Columns: showFullFieldsResultColumns(),
		Rows:    rows,
	}, nil
}

func (c *mysqlConn) showFullFieldColumns(ctx context.Context, tableName, createSQL string) ([]showFullFieldColumn, error) {
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.table_info("+quoteIdent(tableName)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	upperCreateSQL := strings.ToUpper(createSQL)
	columns := []showFullFieldColumn{}
	for rows.Next() {
		var cid int
		var columnName string
		var declaredType string
		var notNull int
		var defaultValue sql.NullString
		var primaryKey int
		if err := rows.Scan(&cid, &columnName, &declaredType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, err
		}
		dataType, columnType := informationSchemaColumnTypes(declaredType)
		isNullable := "YES"
		if notNull != 0 || primaryKey != 0 {
			isNullable = "NO"
		}
		columnKey := ""
		if primaryKey != 0 {
			columnKey = "PRI"
		}
		extra := ""
		if primaryKey != 0 && strings.Contains(upperCreateSQL, "AUTOINCREMENT") {
			extra = "auto_increment"
		}
		var defaultArg any
		if defaultValue.Valid {
			defaultArg = mysqlColumnDefaultMetadataValue(defaultValue.String)
		}
		columns = append(columns, showFullFieldColumn{
			name:       columnName,
			dataType:   dataType,
			columnType: columnType,
			isNullable: isNullable,
			columnKey:  columnKey,
			defaultVal: defaultArg,
			extra:      extra,
		})
		_ = cid
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func (c *mysqlConn) showFullFieldIndexKeys(ctx context.Context, tableName string) (map[string]string, error) {
	indexRows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.index_list("+quoteIdent(tableName)+")")
	if err != nil {
		return nil, err
	}
	defer indexRows.Close()

	keys := map[string]string{}
	for indexRows.Next() {
		var seq int
		var sqliteIndexName string
		var unique int
		var origin string
		var partial int
		if err := indexRows.Scan(&seq, &sqliteIndexName, &unique, &origin, &partial); err != nil {
			return nil, err
		}
		_ = seq
		_ = origin
		_ = partial
		if sqliteIndexName == "" {
			continue
		}
		if err := c.addShowFullFieldIndexKeys(ctx, keys, tableName, sqliteIndexName, unique); err != nil {
			return nil, err
		}
	}
	if err := indexRows.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

func (c *mysqlConn) addShowFullFieldIndexKeys(ctx context.Context, keys map[string]string, tableName, sqliteIndexName string, unique int) error {
	metadata, hasMetadata := c.server.lookupMySQLIndexMetadataBySQLiteName(tableName, sqliteIndexName)
	columnRows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.index_info("+quoteIdent(sqliteIndexName)+")")
	if err != nil {
		return err
	}
	defer columnRows.Close()

	for columnRows.Next() {
		var seqno int
		var cid int
		var columnName sql.NullString
		if err := columnRows.Scan(&seqno, &cid, &columnName); err != nil {
			return err
		}
		_ = cid
		insertColumnName := columnName.String
		if hasMetadata && seqno < len(metadata.Columns) && metadata.Columns[seqno].ColumnName != "" {
			insertColumnName = metadata.Columns[seqno].ColumnName
		}
		if insertColumnName == "" {
			continue
		}
		key := showFullFieldColumnKey(insertColumnName)
		current := keys[key]
		if current == "PRI" || current == "UNI" {
			continue
		}
		if unique != 0 {
			keys[key] = "UNI"
		} else if current == "" {
			keys[key] = "MUL"
		}
	}
	return columnRows.Err()
}

func showFullFieldsResultColumns() []resultColumn {
	return []resultColumn{
		{Name: "Field", Type: fieldTypeVarString},
		{Name: "Type", Type: fieldTypeVarString},
		{Name: "Collation", Type: fieldTypeVarString},
		{Name: "Null", Type: fieldTypeVarString},
		{Name: "Key", Type: fieldTypeVarString},
		{Name: "Default", Type: fieldTypeVarString},
		{Name: "Extra", Type: fieldTypeVarString},
		{Name: "Privileges", Type: fieldTypeVarString},
		{Name: "Comment", Type: fieldTypeVarString},
	}
}

func columnUsesTextCollation(dataType, columnType string) bool {
	return dataType == "text" || dataType == "varchar" || dataType == "char" || strings.Contains(columnType, "char")
}

func showFullFieldColumnKey(columnName string) string {
	return strings.ToLower(columnName)
}

func sqlLikeMatch(value, pattern string) bool {
	return sqlLikeMatchAt(value, pattern, 0, 0)
}

func sqlLikeMatchAt(value, pattern string, valuePos, patternPos int) bool {
	for patternPos < len(pattern) {
		switch pattern[patternPos] {
		case '%':
			for patternPos < len(pattern) && pattern[patternPos] == '%' {
				patternPos++
			}
			if patternPos == len(pattern) {
				return true
			}
			for i := valuePos; i <= len(value); i++ {
				if sqlLikeMatchAt(value, pattern, i, patternPos) {
					return true
				}
			}
			return false
		case '_':
			if valuePos >= len(value) {
				return false
			}
			valuePos++
			patternPos++
		default:
			if valuePos >= len(value) || !equalASCIIFoldByte(value[valuePos], pattern[patternPos]) {
				return false
			}
			valuePos++
			patternPos++
		}
	}
	return valuePos == len(value)
}

func equalASCIIFoldByte(a, b byte) bool {
	if 'A' <= a && a <= 'Z' {
		a += 'a' - 'A'
	}
	if 'A' <= b && b <= 'Z' {
		b += 'a' - 'A'
	}
	return a == b
}

func (c *mysqlConn) showCreateTable(ctx context.Context, sqlText string) (resultSet, error) {
	c.server.stats.recordShowCreateTableQuery()
	tableName, ok := parseShowCreateTable(sqlText)
	if !ok {
		return resultSet{}, c.server.unsupportedError(sqlText)
	}
	var createSQL sql.NullString
	err := c.sqliteConn.QueryRowContext(ctx, `
SELECT sql
FROM main.sqlite_master
WHERE type IN ('table', 'view')
  AND name = ?`, tableName).Scan(&createSQL)
	if err != nil {
		if err == sql.ErrNoRows {
			return resultSet{}, errPacket(mysqlErrNoSuchTable, "42S02", "Table '"+c.currentDB+"."+tableName+"' doesn't exist")
		}
		return resultSet{}, err
	}
	createText := ""
	if originalDDL, ok := c.server.lookupMySQLTableDDL(tableName); ok {
		createText = originalDDL
	} else if createSQL.Valid {
		createText = c.mysqlShowCreateTableSQL(createSQL.String)
	}
	return resultSet{
		Columns: []resultColumn{
			{Name: "Table", Type: fieldTypeVarString},
			{Name: "Create Table", Type: fieldTypeVarString},
		},
		Rows: [][]any{{tableName, createText}},
	}, nil
}

func (c *mysqlConn) mysqlShowCreateTableSQL(createSQL string) string {
	if strings.TrimSpace(createSQL) == "" {
		return createSQL
	}
	upper := strings.ToUpper(createSQL)
	if strings.Contains(upper, " ENGINE=") {
		return createSQL
	}
	charset, ok := c.compatVariable("character_set_database")
	if !ok || charset == "" {
		charset = c.characterSetConnection
	}
	collation, ok := c.compatVariable("collation_database")
	if !ok || collation == "" {
		collation = c.collationConnection
	}
	return createSQL + " ENGINE=InnoDB DEFAULT CHARSET=" + charset + " COLLATE=" + collation
}

func (c *mysqlConn) showKeys(ctx context.Context, sqlText string) (resultSet, error) {
	c.server.stats.recordShowKeysQuery()
	tableName, ok := parseShowKeys(sqlText)
	if !ok {
		return resultSet{}, c.server.unsupportedError(sqlText)
	}
	exists, err := c.refreshInformationSchemaTable(ctx, tableName)
	if err != nil {
		return resultSet{}, err
	}
	if !exists {
		return resultSet{}, errPacket(mysqlErrNoSuchTable, "42S02", "Table '"+c.currentDB+"."+tableName+"' doesn't exist")
	}
	return c.querySQLite(ctx, `
SELECT
  s.TABLE_NAME AS "Table",
  s.NON_UNIQUE AS "Non_unique",
  s.INDEX_NAME AS "Key_name",
  s.SEQ_IN_INDEX AS "Seq_in_index",
  s.COLUMN_NAME AS "Column_name",
  'A' AS "Collation",
  NULL AS "Cardinality",
  s.SUB_PART AS "Sub_part",
  NULL AS "Packed",
  CASE WHEN c.IS_NULLABLE = 'YES' THEN 'YES' ELSE '' END AS "Null",
  s.INDEX_TYPE AS "Index_type",
  '' AS "Comment",
  '' AS "Index_comment",
  COALESCE(s.VISIBLE, 'YES') AS "Visible",
  s.EXPRESSION AS "Expression"
FROM "information_schema"."statistics" s
LEFT JOIN "information_schema"."columns" c
  ON c.TABLE_SCHEMA = s.TABLE_SCHEMA
 AND c.TABLE_NAME = s.TABLE_NAME
 AND c.COLUMN_NAME = s.COLUMN_NAME
WHERE s.TABLE_SCHEMA = ?
  AND s.TABLE_NAME = ?
ORDER BY
  CASE WHEN s.INDEX_NAME = 'PRIMARY' THEN 0 ELSE 1 END,
  s.INDEX_NAME,
  s.SEQ_IN_INDEX`, c.currentDB, tableName)
}

func (c *mysqlConn) tableExists(ctx context.Context, tableName string) (bool, error) {
	var count int
	err := c.sqliteConn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM main.sqlite_master
WHERE type IN ('table', 'view')
  AND name = ?`, tableName).Scan(&count)
	return count > 0, err
}

func parseShowFullFields(sqlText string) (tableName, likePattern string, ok bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "SHOW") {
		return "", "", false
	}
	_ = consumeKeyword(sqlText, &pos, "FULL")
	if !consumeKeyword(sqlText, &pos, "FIELDS") && !consumeKeyword(sqlText, &pos, "COLUMNS") {
		return "", "", false
	}
	if !consumeKeyword(sqlText, &pos, "FROM") {
		return "", "", false
	}
	tableName, pos, ok = readSQLQualifiedName(sqlText, pos)
	if !ok {
		return "", "", false
	}
	if consumeKeyword(sqlText, &pos, "LIKE") {
		value, next, ok := readSQLValueToken(sqlText, pos)
		if !ok {
			return "", "", false
		}
		likePattern = unquoteSQLWord(value)
		pos = next
	}
	return tableName, likePattern, true
}

func parseShowCreateTable(sqlText string) (string, bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "SHOW") ||
		!consumeKeyword(sqlText, &pos, "CREATE") ||
		!consumeKeyword(sqlText, &pos, "TABLE") {
		return "", false
	}
	tableName, pos, ok := readSQLQualifiedName(sqlText, pos)
	if !ok {
		return "", false
	}
	return tableName, true
}

func parseShowKeys(sqlText string) (string, bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "SHOW") {
		return "", false
	}
	if !consumeKeyword(sqlText, &pos, "KEYS") &&
		!consumeKeyword(sqlText, &pos, "INDEX") &&
		!consumeKeyword(sqlText, &pos, "INDEXES") {
		return "", false
	}
	if !consumeKeyword(sqlText, &pos, "FROM") {
		return "", false
	}
	tableName, pos, ok := readSQLQualifiedName(sqlText, pos)
	if !ok {
		return "", false
	}
	return tableName, true
}

func readSQLQualifiedName(sqlText string, pos int) (string, int, bool) {
	name, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return "", pos, false
	}
	for {
		next := skipSQLSpaces(sqlText, pos)
		if next >= len(sqlText) || sqlText[next] != '.' {
			break
		}
		part, partEnd, ok := readSQLNameToken(sqlText, next+1)
		if !ok {
			return "", pos, false
		}
		name = part
		pos = partEnd
	}
	return unquoteSQLWord(name), pos, true
}

func readSQLValueToken(sqlText string, pos int) (string, int, bool) {
	pos = skipSQLSpaces(sqlText, pos)
	end := consumeSQLValue(sqlText, pos)
	if end < 0 {
		return "", pos, false
	}
	return sqlText[pos:end], end, true
}

func advisoryLockName(expr string) (string, bool) {
	name, end, ok := readSQLIdentifier(expr, 0)
	if !ok || (!strings.EqualFold(name, "GET_LOCK") && !strings.EqualFold(name, "RELEASE_LOCK")) {
		return "", false
	}
	callStart := skipSQLSpaces(expr, end)
	callEnd, ok := parenthesizedSQLSpan(expr, callStart)
	if !ok {
		return "", false
	}
	args := splitSQLTopLevelList(expr[callStart+1 : callEnd-1])
	if len(args) == 0 {
		return "", false
	}
	return unquoteSQLWord(strings.TrimSpace(args[0])), true
}
