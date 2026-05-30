package mysqlmock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type mysqlAutoIncrementInsertValue struct {
	Value     uint64
	Generated bool
}

type cachedSQLiteAutoIncrementTable struct {
	SchemaVersion uint64
	Uses          bool
}

func (s *Server) recordMySQLAutoIncrementAllocation(tableName string, value uint64) bool {
	if tableName == "" || value == 0 {
		return false
	}
	key := tableMetadataKey(tableName)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.autoIncrement == nil {
		s.autoIncrement = map[string]uint64{}
	}
	if value <= s.autoIncrement[key] {
		return false
	}
	s.autoIncrement[key] = value
	return true
}

func (s *Server) mysqlAutoIncrementAllocation(tableName string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.autoIncrement[tableMetadataKey(tableName)]
}

func (c *mysqlConn) recordMySQLAutoIncrementAllocation(ctx context.Context, query string, result okResult) {
	if result.LastInsertID == 0 {
		return
	}
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("mysql.auto_increment.record_allocation", time.Since(start))
	}()

	tableName, ok := parseInsertTargetTable(query)
	if !ok {
		return
	}
	usesAutoIncrement, err := c.sqliteTableUsesAutoincrement(ctx, tableName)
	if err != nil || !usesAutoIncrement {
		return
	}
	if c.server.recordMySQLAutoIncrementAllocation(tableName, result.LastInsertID) {
		c.markMySQLAutoIncrementRestoreTable(tableName)
	}
}

func (c *mysqlConn) restoreMySQLAutoIncrementSequences(ctx context.Context) error {
	tableNames := c.mysqlAutoIncrementRestoreTableNames()
	for _, tableName := range tableNames {
		value := c.server.mysqlAutoIncrementAllocation(tableName)
		if value == 0 {
			continue
		}
		usesAutoIncrement, err := c.sqliteTableUsesAutoincrement(ctx, tableName)
		if err != nil {
			return err
		}
		if !usesAutoIncrement {
			continue
		}
		if err := c.setSQLiteSequenceAtLeast(ctx, tableName, value); err != nil {
			return err
		}
	}
	return nil
}

func (c *mysqlConn) restoreMySQLAutoIncrementSequencesWithTiming(ctx context.Context) error {
	start := time.Now()
	err := c.restoreMySQLAutoIncrementSequences(ctx)
	c.server.stats.recordPhaseTiming("mysql.auto_increment.restore", time.Since(start))
	return err
}

func (c *mysqlConn) sqliteTableUsesAutoincrement(ctx context.Context, tableName string) (bool, error) {
	version := c.server.currentSchemaVersion()
	cacheKey := normalizedTableCacheKey(tableName)
	c.server.metadataMu.Lock()
	if cached, ok := c.server.sqliteAutoIncrementTables[cacheKey]; ok && cached.SchemaVersion == version {
		c.server.metadataMu.Unlock()
		return cached.Uses, nil
	}
	c.server.metadataMu.Unlock()

	var createSQL sql.NullString
	start := time.Now()
	err := c.sqliteConn.QueryRowContext(ctx, `
SELECT sql
FROM main.sqlite_master
WHERE type = 'table'
  AND name = ?`, unquoteSQLWord(tableName)).Scan(&createSQL)
	c.server.stats.recordPhaseTiming("mysql.auto_increment.sqlite_table_lookup", time.Since(start))
	if errors.Is(err, sql.ErrNoRows) {
		c.server.cacheSQLiteTableAutoincrement(cacheKey, version, false)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	uses := createSQL.Valid && containsSQLIdentifier(createSQL.String, "AUTOINCREMENT")
	c.server.cacheSQLiteTableAutoincrement(cacheKey, version, uses)
	return uses, nil
}

func (s *Server) cacheSQLiteTableAutoincrement(cacheKey string, version uint64, uses bool) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	if s.sqliteAutoIncrementTables == nil {
		s.sqliteAutoIncrementTables = map[string]cachedSQLiteAutoIncrementTable{}
	}
	s.sqliteAutoIncrementTables[cacheKey] = cachedSQLiteAutoIncrementTable{
		SchemaVersion: version,
		Uses:          uses,
	}
}

func (c *mysqlConn) markMySQLAutoIncrementRestoreTable(tableName string) {
	if c.statusFlags&serverStatusInTrans == 0 {
		return
	}
	if c.autoIncrementRestoreTables == nil {
		c.autoIncrementRestoreTables = map[string]string{}
	}
	c.autoIncrementRestoreTables[tableMetadataKey(tableName)] = unquoteSQLWord(tableName)
}

func (c *mysqlConn) mysqlAutoIncrementRestoreTableNames() []string {
	tableNames := make([]string, 0, len(c.autoIncrementRestoreTables))
	for _, tableName := range c.autoIncrementRestoreTables {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	return tableNames
}

func (c *mysqlConn) clearMySQLAutoIncrementRestoreTables() {
	c.autoIncrementRestoreTables = nil
}

func (c *mysqlConn) setSQLiteSequenceAtLeast(ctx context.Context, tableName string, value uint64) error {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("mysql.auto_increment.sequence_update", time.Since(start))
	}()

	if value > math.MaxInt64 {
		value = math.MaxInt64
	}
	seq := int64(value)
	result, err := c.sqliteConn.ExecContext(ctx, `
UPDATE sqlite_sequence
SET seq = CASE WHEN seq < ? THEN ? ELSE seq END
WHERE name = ?`, seq, seq, unquoteSQLWord(tableName))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 0 {
		return nil
	}
	_, err = c.sqliteConn.ExecContext(ctx, `
INSERT OR IGNORE INTO sqlite_sequence (name, seq)
VALUES (?, ?)`, unquoteSQLWord(tableName), seq)
	return err
}

func parseInsertTargetTable(sqlText string) (string, bool) {
	pos := skipSQLSpaces(sqlText, 0)
	replace := consumeKeyword(sqlText, &pos, "REPLACE")
	if !replace && !consumeKeyword(sqlText, &pos, "INSERT") {
		return "", false
	}
	if !replace {
		for {
			start := pos
			switch {
			case consumeKeyword(sqlText, &pos, "LOW_PRIORITY"):
			case consumeKeyword(sqlText, &pos, "DELAYED"):
			case consumeKeyword(sqlText, &pos, "HIGH_PRIORITY"):
			case consumeKeyword(sqlText, &pos, "IGNORE"):
			case consumeKeyword(sqlText, &pos, "OR"):
				if _, next, ok := readSQLIdentifier(sqlText, pos); ok {
					pos = next
				} else {
					pos = start
					goto modifiersDone
				}
			default:
				pos = start
				goto modifiersDone
			}
		}
	}

modifiersDone:
	if replace {
		_ = consumeKeyword(sqlText, &pos, "INTO")
	} else if !consumeKeyword(sqlText, &pos, "INTO") {
		return "", false
	}
	tableName, _, ok := readSQLQualifiedName(sqlText, pos)
	if !ok || strings.EqualFold(tableName, "sqlite_sequence") {
		return "", false
	}
	return tableName, true
}

func (c *mysqlConn) needsMySQLAutoIncrementInsertCompatibility(tableColumns []sqliteTableColumn, tableName string) bool {
	metadata, ok := c.server.lookupMySQLAutoIncrementColumn(tableName)
	if !ok {
		return false
	}
	return !sqliteTableColumnsUseRowIDAutoIncrement(tableColumns, metadata.ColumnName)
}

func (c *mysqlConn) applyMySQLAutoIncrementInsertValue(ctx context.Context, tableName string, tableColumns []sqliteTableColumn, columns *[]string, values *[]any) (mysqlAutoIncrementInsertValue, error) {
	metadata, ok := c.server.lookupMySQLAutoIncrementColumn(tableName)
	if !ok || sqliteTableColumnsUseRowIDAutoIncrement(tableColumns, metadata.ColumnName) {
		return mysqlAutoIncrementInsertValue{}, nil
	}
	columnIndex := insertColumnIndex(*columns, metadata.ColumnName)
	if columnIndex >= 0 {
		if columnIndex >= len(*values) {
			return mysqlAutoIncrementInsertValue{}, nil
		}
		value := (*values)[columnIndex]
		if !shouldGenerateAutoIncrementValue(value) {
			if explicit, ok := positiveAutoIncrementValue(value); ok {
				return mysqlAutoIncrementInsertValue{Value: explicit}, nil
			}
			return mysqlAutoIncrementInsertValue{}, nil
		}
		next, err := c.nextMySQLAutoIncrementValue(ctx, tableName, metadata.ColumnName)
		if err != nil {
			return mysqlAutoIncrementInsertValue{}, err
		}
		(*values)[columnIndex] = int64(next)
		return mysqlAutoIncrementInsertValue{Value: next, Generated: true}, nil
	}

	next, err := c.nextMySQLAutoIncrementValue(ctx, tableName, metadata.ColumnName)
	if err != nil {
		return mysqlAutoIncrementInsertValue{}, err
	}
	*columns = append(*columns, metadata.ColumnName)
	*values = append(*values, int64(next))
	return mysqlAutoIncrementInsertValue{Value: next, Generated: true}, nil
}

func (c *mysqlConn) nextMySQLAutoIncrementValue(ctx context.Context, tableName, columnName string) (uint64, error) {
	start := time.Now()
	defer func() {
		c.server.stats.recordPhaseTiming("mysql.auto_increment.next_value", time.Since(start))
	}()

	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(unquoteSQLWord(columnName)), quoteIdent(unquoteSQLWord(tableName)))
	var maxValue sql.NullInt64
	if err := c.sqliteConn.QueryRowContext(ctx, query).Scan(&maxValue); err != nil {
		return 0, err
	}

	var base uint64
	if maxValue.Valid && maxValue.Int64 > 0 {
		base = uint64(maxValue.Int64)
	}
	if allocated := c.server.mysqlAutoIncrementAllocation(tableName); allocated > base {
		base = allocated
	}
	if base >= uint64(math.MaxInt64) {
		return 0, sqlCompatErrorf("AUTO_INCREMENT value overflow for %s.%s", unquoteSQLWord(tableName), unquoteSQLWord(columnName))
	}
	return base + 1, nil
}

func sqliteTableColumnsUseRowIDAutoIncrement(tableColumns []sqliteTableColumn, columnName string) bool {
	column, ok := findSQLiteTableColumn(tableColumns, columnName)
	if !ok || column.PK != 1 || !strings.EqualFold(strings.TrimSpace(column.Type), "INTEGER") {
		return false
	}
	pkColumns := 0
	for _, tableColumn := range tableColumns {
		if tableColumn.PK > 0 {
			pkColumns++
		}
	}
	return pkColumns == 1
}

func insertColumnIndex(columns []string, columnName string) int {
	for i, column := range columns {
		if strings.EqualFold(unquoteSQLWord(column), columnName) {
			return i
		}
	}
	return -1
}

func shouldGenerateAutoIncrementValue(value any) bool {
	if value == nil {
		return true
	}
	id, err := int64Value(value)
	return err == nil && id == 0
}

func positiveAutoIncrementValue(value any) (uint64, bool) {
	id, err := int64Value(value)
	if err != nil || id <= 0 {
		return 0, false
	}
	return uint64(id), true
}
