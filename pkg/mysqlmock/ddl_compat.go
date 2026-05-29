package mysqlmock

import (
	"context"
	"fmt"
	"strings"
)

func (c *mysqlConn) execMySQLDDLCompatibility(ctx context.Context, sqlText string) (okResult, bool, error) {
	if isDropDatabaseStatement(sqlText) {
		return okResult{}, true, nil
	}
	if oldName, newName, ok := parseRenameTable(sqlText); ok {
		resp, err := c.execSQLite(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quoteIdent(oldName), quoteIdent(newName)))
		if err == nil {
			c.server.renameMySQLTableIndexMetadata(oldName, newName)
			c.server.renameMySQLTableDDL(oldName, newName)
			c.server.invalidateMySQLTableDDL(newName)
		}
		return resp, true, err
	}
	if tableName, oldName, newName, ok := parseAlterTableRenameIndex(sqlText); ok {
		resp, handled, err := c.renameIndex(ctx, tableName, oldName, newName)
		if err == nil {
			c.server.renameMySQLIndexMetadata(tableName, oldName, newName)
		}
		return resp, handled, err
	}
	if tableName, indexName, ok := parseAlterTableDropIndex(sqlText); ok {
		resp, err := c.execSQLite(ctx, "DROP INDEX "+quoteIdent(c.server.sqliteIndexNameForMySQL(tableName, indexName)))
		if err == nil {
			c.server.dropMySQLIndexMetadata(tableName, indexName)
		}
		return resp, true, err
	}
	if tableName, indexName, ok := parseDropIndexOnTable(sqlText); ok {
		resp, err := c.execSQLite(ctx, "DROP INDEX "+quoteIdent(c.server.sqliteIndexNameForMySQL(tableName, indexName)))
		if err == nil {
			c.server.dropMySQLIndexMetadata(tableName, indexName)
		}
		return resp, true, err
	}
	if tableName, indexName, visible, ok := parseAlterTableAlterIndexVisibility(sqlText); ok {
		c.server.setMySQLIndexVisibility(tableName, indexName, visible)
		c.server.bumpSchemaVersion()
		return okResult{}, true, nil
	}
	if tableName, oldName, newName, ok := parseAlterTableChangeColumn(sqlText); ok {
		if strings.EqualFold(oldName, newName) {
			c.server.invalidateMySQLTableDDL(tableName)
			return okResult{}, true, nil
		}
		resp, err := c.execSQLite(ctx, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", quoteIdent(tableName), quoteIdent(oldName), quoteIdent(newName)))
		if err == nil {
			c.server.invalidateMySQLTableDDL(tableName)
		}
		return resp, true, err
	}
	if tableName, columnName, ok := parseAlterTableModifyColumn(sqlText); ok {
		exists, err := c.columnExists(ctx, tableName, columnName)
		if err != nil {
			return okResult{}, true, err
		}
		if !exists {
			return okResult{}, true, errPacket(mysqlErrUnknown, "42S22", "Unknown column '"+columnName+"'")
		}
		c.server.invalidateMySQLTableDDL(tableName)
		return okResult{}, true, nil
	}
	return okResult{}, false, nil
}

func isDropDatabaseStatement(sqlText string) bool {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "DROP") {
		return false
	}
	if !consumeKeyword(sqlText, &pos, "DATABASE") && !consumeKeyword(sqlText, &pos, "SCHEMA") {
		return false
	}
	if consumeKeyword(sqlText, &pos, "IF") && !consumeKeyword(sqlText, &pos, "EXISTS") {
		return false
	}
	_, next, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return false
	}
	pos = next
	pos = skipSQLSpaces(sqlText, pos)
	if pos < len(sqlText) && sqlText[pos] == ';' {
		pos = skipSQLSpaces(sqlText, pos+1)
	}
	return pos == len(sqlText)
}

func (c *mysqlConn) renameIndex(ctx context.Context, tableName, oldName, newName string) (okResult, bool, error) {
	index, err := c.sqliteIndexDefinition(ctx, tableName, oldName)
	if err != nil {
		return okResult{}, true, err
	}
	if _, err := c.execSQLite(ctx, "DROP INDEX "+quoteIdent(index.Name)); err != nil {
		return okResult{}, true, err
	}
	create := "CREATE "
	if index.Unique {
		create += "UNIQUE "
	}
	create += fmt.Sprintf("INDEX %s ON %s (%s)", quoteIdent(sqliteIndexName(tableName, newName)), quoteIdent(tableName), joinQuoted(index.Columns))
	resp, err := c.execSQLite(ctx, create)
	return resp, true, err
}

type sqliteIndexDefinition struct {
	Name    string
	Unique  bool
	Columns []string
}

func (c *mysqlConn) sqliteIndexDefinition(ctx context.Context, tableName, indexName string) (sqliteIndexDefinition, error) {
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.index_list("+quoteIdent(tableName)+")")
	if err != nil {
		return sqliteIndexDefinition{}, err
	}
	defer rows.Close()

	var found bool
	var sqliteName string
	var unique bool
	expectedSQLiteName := c.server.sqliteIndexNameForMySQL(tableName, indexName)
	for rows.Next() {
		var seq int
		var name string
		var uniqueInt int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &uniqueInt, &origin, &partial); err != nil {
			return sqliteIndexDefinition{}, err
		}
		_ = seq
		_ = origin
		_ = partial
		if strings.EqualFold(name, expectedSQLiteName) || strings.EqualFold(name, indexName) {
			found = true
			sqliteName = name
			unique = uniqueInt != 0
			break
		}
	}
	if err := rows.Err(); err != nil {
		return sqliteIndexDefinition{}, err
	}
	if !found {
		return sqliteIndexDefinition{}, errPacket(mysqlErrUnknown, "42000", "Key '"+indexName+"' doesn't exist in table '"+tableName+"'")
	}
	columns, err := c.indexColumns(ctx, sqliteName)
	if err != nil {
		return sqliteIndexDefinition{}, err
	}
	if len(columns) == 0 {
		return sqliteIndexDefinition{}, errPacket(mysqlErrUnknown, "HY000", "Unsupported expression index: "+indexName)
	}
	return sqliteIndexDefinition{Name: sqliteName, Unique: unique, Columns: columns}, nil
}

func (c *mysqlConn) columnExists(ctx context.Context, tableName, columnName string) (bool, error) {
	columns, err := c.tableColumns(ctx, tableName)
	if err != nil {
		return false, err
	}
	for _, column := range columns {
		if strings.EqualFold(column.Name, columnName) {
			return true, nil
		}
	}
	return false, nil
}

func parseRenameTable(sqlText string) (oldName, newName string, ok bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "RENAME") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", "", false
	}
	oldName, pos, ok = readSQLQualifiedName(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "TO") {
		return "", "", false
	}
	newName, _, ok = readSQLQualifiedName(sqlText, pos)
	return oldName, newName, ok
}

func parseAlterTableRenameIndex(sqlText string) (tableName, oldName, newName string, ok bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "ALTER") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", "", "", false
	}
	tableName, pos, ok = readSQLQualifiedName(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "RENAME") {
		return "", "", "", false
	}
	if !consumeKeyword(sqlText, &pos, "INDEX") && !consumeKeyword(sqlText, &pos, "KEY") {
		return "", "", "", false
	}
	oldName, pos, ok = readSQLNameToken(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "TO") {
		return "", "", "", false
	}
	newName, _, ok = readSQLNameToken(sqlText, pos)
	return tableName, unquoteSQLWord(oldName), unquoteSQLWord(newName), ok
}

func parseAlterTableDropIndex(sqlText string) (tableName, indexName string, ok bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "ALTER") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", "", false
	}
	tableName, pos, ok = readSQLQualifiedName(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "DROP") {
		return "", "", false
	}
	if !consumeKeyword(sqlText, &pos, "INDEX") && !consumeKeyword(sqlText, &pos, "KEY") {
		return "", "", false
	}
	indexName, _, ok = readSQLNameToken(sqlText, pos)
	return tableName, unquoteSQLWord(indexName), ok
}

func parseDropIndexOnTable(sqlText string) (tableName, indexName string, ok bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "DROP") || !consumeKeyword(sqlText, &pos, "INDEX") {
		return "", "", false
	}
	indexName, pos, ok = readSQLNameToken(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "ON") {
		return "", "", false
	}
	tableName, _, ok = readSQLQualifiedName(sqlText, pos)
	return tableName, unquoteSQLWord(indexName), ok
}

func parseAlterTableChangeColumn(sqlText string) (tableName, oldName, newName string, ok bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "ALTER") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", "", "", false
	}
	tableName, pos, ok = readSQLQualifiedName(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "CHANGE") {
		return "", "", "", false
	}
	_ = consumeKeyword(sqlText, &pos, "COLUMN")
	oldToken, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return "", "", "", false
	}
	newToken, _, ok := readSQLNameToken(sqlText, pos)
	return tableName, unquoteSQLWord(oldToken), unquoteSQLWord(newToken), ok
}

func parseAlterTableModifyColumn(sqlText string) (tableName, columnName string, ok bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "ALTER") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", "", false
	}
	tableName, pos, ok = readSQLQualifiedName(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "MODIFY") {
		return "", "", false
	}
	_ = consumeKeyword(sqlText, &pos, "COLUMN")
	columnName, _, ok = readSQLNameToken(sqlText, pos)
	return tableName, unquoteSQLWord(columnName), ok
}

func parseAlterTableAlterIndexVisibility(sqlText string) (tableName, indexName, visible string, ok bool) {
	pos := 0
	if !consumeKeyword(sqlText, &pos, "ALTER") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", "", "", false
	}
	tableName, pos, ok = readSQLQualifiedName(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "ALTER") {
		return "", "", "", false
	}
	if !consumeKeyword(sqlText, &pos, "INDEX") && !consumeKeyword(sqlText, &pos, "KEY") {
		return "", "", "", false
	}
	indexToken, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return "", "", "", false
	}
	switch {
	case consumeKeyword(sqlText, &pos, "VISIBLE"):
		return tableName, unquoteSQLWord(indexToken), "YES", true
	case consumeKeyword(sqlText, &pos, "INVISIBLE"):
		return tableName, unquoteSQLWord(indexToken), "NO", true
	default:
		return "", "", "", false
	}
}
