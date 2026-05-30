package mysqlmock

import (
	"context"
	"database/sql"
	"strings"
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
	c.server.stats.recordShowFullFieldsQuery()
	tableName, likePattern, ok := parseShowFullFields(sqlText)
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

	query := `
SELECT
  c.COLUMN_NAME AS "Field",
  c.COLUMN_TYPE AS "Type",
  CASE
    WHEN c.DATA_TYPE IN ('text', 'varchar', 'char') OR c.COLUMN_TYPE LIKE '%char%' THEN ?
    ELSE NULL
  END AS "Collation",
  c.IS_NULLABLE AS "Null",
  CASE
    WHEN EXISTS (
      SELECT 1 FROM "information_schema"."statistics" s
      WHERE s.TABLE_SCHEMA = c.TABLE_SCHEMA
        AND s.TABLE_NAME = c.TABLE_NAME
        AND s.COLUMN_NAME = c.COLUMN_NAME
        AND s.INDEX_NAME = 'PRIMARY'
    ) THEN 'PRI'
    WHEN EXISTS (
      SELECT 1 FROM "information_schema"."statistics" s
      WHERE s.TABLE_SCHEMA = c.TABLE_SCHEMA
        AND s.TABLE_NAME = c.TABLE_NAME
        AND s.COLUMN_NAME = c.COLUMN_NAME
        AND s.NON_UNIQUE = 0
    ) THEN 'UNI'
    WHEN EXISTS (
      SELECT 1 FROM "information_schema"."statistics" s
      WHERE s.TABLE_SCHEMA = c.TABLE_SCHEMA
        AND s.TABLE_NAME = c.TABLE_NAME
        AND s.COLUMN_NAME = c.COLUMN_NAME
    ) THEN 'MUL'
    ELSE c.COLUMN_KEY
  END AS "Key",
  c.COLUMN_DEFAULT AS "Default",
  c.EXTRA AS "Extra",
  'select,insert,update,references' AS "Privileges",
  '' AS "Comment"
FROM "information_schema"."columns" c
WHERE c.TABLE_SCHEMA = ?
  AND c.TABLE_NAME = ?`
	args := []any{c.collationConnection, c.currentDB, tableName}
	if likePattern != "" {
		query += "\n  AND c.COLUMN_NAME LIKE ?"
		args = append(args, likePattern)
	}
	query += "\nORDER BY c.ORDINAL_POSITION"
	return c.querySQLite(ctx, query, args...)
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
