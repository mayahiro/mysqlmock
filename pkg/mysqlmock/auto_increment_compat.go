package mysqlmock

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
)

func (s *Server) recordMySQLAutoIncrementAllocation(tableName string, value uint64) {
	if tableName == "" || value == 0 {
		return
	}
	key := tableMetadataKey(tableName)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.autoIncrement == nil {
		s.autoIncrement = map[string]uint64{}
	}
	if value > s.autoIncrement[key] {
		s.autoIncrement[key] = value
	}
}

func (s *Server) mysqlAutoIncrementAllocations() map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]uint64, len(s.autoIncrement))
	for tableName, value := range s.autoIncrement {
		out[tableName] = value
	}
	return out
}

func (c *mysqlConn) recordMySQLAutoIncrementAllocation(query string, result okResult) {
	if result.LastInsertID == 0 {
		return
	}
	tableName, ok := parseInsertTargetTable(query)
	if !ok {
		return
	}
	c.server.recordMySQLAutoIncrementAllocation(tableName, result.LastInsertID)
}

func (c *mysqlConn) restoreMySQLAutoIncrementSequences(ctx context.Context) error {
	for tableName, value := range c.server.mysqlAutoIncrementAllocations() {
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

func (c *mysqlConn) sqliteTableUsesAutoincrement(ctx context.Context, tableName string) (bool, error) {
	var createSQL sql.NullString
	err := c.sqliteConn.QueryRowContext(ctx, `
SELECT sql
FROM main.sqlite_master
WHERE type = 'table'
  AND name = ?`, unquoteSQLWord(tableName)).Scan(&createSQL)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return createSQL.Valid && containsSQLIdentifier(createSQL.String, "AUTOINCREMENT"), nil
}

func (c *mysqlConn) setSQLiteSequenceAtLeast(ctx context.Context, tableName string, value uint64) error {
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
