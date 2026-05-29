package mysqlmock

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func isInformationSchemaQuery(upperSQL string) bool {
	unquoted := strings.NewReplacer("`", "", `"`, "").Replace(upperSQL)
	return strings.Contains(unquoted, "INFORMATION_SCHEMA.")
}

func (c *mysqlConn) queryInformationSchema(ctx context.Context, sqlText string, args ...any) (resultSet, error) {
	resp, err := c.queryInformationSchemaText(ctx, sqlText, false, args...)
	if err != nil {
		return resultSet{}, err
	}
	switch v := resp.(type) {
	case resultSet:
		return v, nil
	case *sqliteResultSet:
		return v.materialize()
	default:
		return resultSet{}, errPacket(mysqlErrUnknown, "HY000", "Unsupported information_schema query response")
	}
}

func (c *mysqlConn) queryInformationSchemaText(ctx context.Context, sqlText string, stream bool, args ...any) (any, error) {
	if tableName, ok := informationSchemaTargetTable(sqlText, args); ok {
		if _, err := c.refreshInformationSchemaTable(ctx, tableName); err != nil {
			return nil, err
		}
	} else {
		if err := c.refreshInformationSchema(ctx); err != nil {
			return nil, err
		}
	}
	query := c.server.translateSQLCached(rewriteInformationSchemaSQL(sqlText, c.currentDB))
	return c.querySQLiteText(ctx, query, stream, args...)
}

func (c *mysqlConn) refreshInformationSchema(ctx context.Context) error {
	version := c.server.currentSchemaVersion()
	if c.informationSchemaCache.hasFullRefresh(version) {
		return nil
	}
	if err := c.prepareInformationSchema(ctx); err != nil {
		return err
	}
	if err := c.withInformationSchemaRefreshBatch(ctx, func() error {
		if err := c.clearInformationSchema(ctx); err != nil {
			return err
		}
		if err := c.refreshInformationSchemaSchemata(ctx); err != nil {
			return err
		}

		rows, err := c.sqliteConn.QueryContext(ctx, `
SELECT type, name, sql
FROM main.sqlite_master
WHERE type IN ('table', 'view')
  AND name NOT LIKE 'sqlite_%'
ORDER BY name`)
		if err != nil {
			return fmt.Errorf("list sqlite tables for information_schema: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var tableType string
			var tableName string
			var createSQL sql.NullString
			if err := rows.Scan(&tableType, &tableName, &createSQL); err != nil {
				return fmt.Errorf("scan sqlite table for information_schema: %w", err)
			}
			if err := c.insertInformationSchemaTableMetadata(ctx, tableName, tableType, createSQL.String); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("list sqlite tables for information_schema: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	c.informationSchemaCache.markFullRefresh(version)
	return nil
}

func (c *mysqlConn) refreshInformationSchemaTable(ctx context.Context, tableName string) (bool, error) {
	version := c.server.currentSchemaVersion()
	if exists, ok := c.informationSchemaCache.tableExists(tableName, version); ok {
		return exists, nil
	}
	if c.informationSchemaCache.hasFullRefresh(version) {
		return c.informationSchemaCachedTableExists(ctx, tableName)
	}
	if err := c.prepareInformationSchema(ctx); err != nil {
		return false, err
	}

	exists := false
	if err := c.withInformationSchemaRefreshBatch(ctx, func() error {
		if err := c.refreshInformationSchemaSchemata(ctx); err != nil {
			return err
		}
		if err := c.clearInformationSchemaTable(ctx, tableName); err != nil {
			return err
		}

		var tableType string
		var createSQL sql.NullString
		err := c.sqliteConn.QueryRowContext(ctx, `
SELECT type, sql
FROM main.sqlite_master
WHERE type IN ('table', 'view')
  AND name = ?
  AND name NOT LIKE 'sqlite_%'`, tableName).Scan(&tableType, &createSQL)
		if err != nil {
			if err == sql.ErrNoRows {
				exists = false
				return nil
			}
			return fmt.Errorf("load sqlite table for information_schema.%s: %w", tableName, err)
		}
		if err := c.insertInformationSchemaTableMetadata(ctx, tableName, tableType, createSQL.String); err != nil {
			return err
		}
		exists = true
		return nil
	}); err != nil {
		return false, err
	}
	c.informationSchemaCache.markTable(tableName, version, exists)
	return exists, nil
}

func (c *mysqlConn) prepareInformationSchema(ctx context.Context) error {
	if err := c.ensureInformationSchemaAttached(ctx); err != nil {
		return err
	}
	if err := c.createInformationSchemaTables(ctx); err != nil {
		return err
	}
	return nil
}

func (c *mysqlConn) withInformationSchemaRefreshBatch(ctx context.Context, refresh func() error) (err error) {
	if _, err := c.sqliteConn.ExecContext(ctx, "SAVEPOINT mysqlmock_information_schema_refresh"); err != nil {
		return fmt.Errorf("start information_schema refresh batch: %w", err)
	}

	released := false
	defer func() {
		if released {
			return
		}
		_, rollbackErr := c.sqliteConn.ExecContext(ctx, "ROLLBACK TO SAVEPOINT mysqlmock_information_schema_refresh")
		_, releaseErr := c.sqliteConn.ExecContext(ctx, "RELEASE SAVEPOINT mysqlmock_information_schema_refresh")
		if err == nil {
			if rollbackErr != nil {
				err = fmt.Errorf("rollback information_schema refresh batch: %w", rollbackErr)
			} else if releaseErr != nil {
				err = fmt.Errorf("release information_schema refresh batch after rollback: %w", releaseErr)
			}
		}
	}()

	if err := refresh(); err != nil {
		return err
	}
	if _, err := c.sqliteConn.ExecContext(ctx, "RELEASE SAVEPOINT mysqlmock_information_schema_refresh"); err != nil {
		return fmt.Errorf("release information_schema refresh batch: %w", err)
	}
	released = true
	return nil
}

func (c *mysqlConn) clearInformationSchema(ctx context.Context) error {
	for _, table := range []string{"schemata", "tables", "columns", "key_column_usage", "statistics", "table_constraints", "referential_constraints", "check_constraints"} {
		if _, err := c.sqliteConn.ExecContext(ctx, `DELETE FROM "information_schema".`+quoteIdent(table)); err != nil {
			return fmt.Errorf("clear information_schema.%s: %w", table, err)
		}
	}
	return nil
}

func (c *mysqlConn) clearInformationSchemaTable(ctx context.Context, tableName string) error {
	deletes := []struct {
		table string
		where string
		args  []any
	}{
		{table: "tables", where: "TABLE_SCHEMA = ? AND TABLE_NAME = ?", args: []any{c.currentDB, tableName}},
		{table: "columns", where: "TABLE_SCHEMA = ? AND TABLE_NAME = ?", args: []any{c.currentDB, tableName}},
		{table: "key_column_usage", where: "TABLE_SCHEMA = ? AND TABLE_NAME = ?", args: []any{c.currentDB, tableName}},
		{table: "statistics", where: "TABLE_SCHEMA = ? AND TABLE_NAME = ?", args: []any{c.currentDB, tableName}},
		{table: "table_constraints", where: "TABLE_SCHEMA = ? AND TABLE_NAME = ?", args: []any{c.currentDB, tableName}},
		{table: "referential_constraints", where: "CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?", args: []any{c.currentDB, tableName}},
	}
	for _, delete := range deletes {
		query := `DELETE FROM "information_schema".` + quoteIdent(delete.table) + ` WHERE ` + delete.where
		if _, err := c.sqliteConn.ExecContext(ctx, query, delete.args...); err != nil {
			return fmt.Errorf("clear information_schema.%s rows for %s: %w", delete.table, tableName, err)
		}
	}
	return nil
}

func (c *mysqlConn) informationSchemaCachedTableExists(ctx context.Context, tableName string) (bool, error) {
	var count int
	err := c.sqliteConn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM "information_schema"."tables"
WHERE TABLE_SCHEMA = ?
  AND TABLE_NAME = ?`, c.currentDB, tableName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (c *mysqlConn) ensureInformationSchemaAttached(ctx context.Context) error {
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		return fmt.Errorf("list sqlite databases: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var seq int
		var name string
		var file string
		if err := rows.Scan(&seq, &name, &file); err != nil {
			return fmt.Errorf("scan sqlite database: %w", err)
		}
		if strings.EqualFold(name, "information_schema") {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list sqlite databases: %w", err)
	}
	if _, err := c.sqliteConn.ExecContext(ctx, `ATTACH DATABASE ':memory:' AS "information_schema"`); err != nil {
		return fmt.Errorf("attach information_schema: %w", err)
	}
	return nil
}

func (c *mysqlConn) createInformationSchemaTables(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS "information_schema"."schemata" (
			CATALOG_NAME TEXT,
			SCHEMA_NAME TEXT,
			DEFAULT_CHARACTER_SET_NAME TEXT,
			DEFAULT_COLLATION_NAME TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS "information_schema"."tables" (
			TABLE_CATALOG TEXT,
			TABLE_SCHEMA TEXT,
			TABLE_NAME TEXT,
			TABLE_TYPE TEXT,
			ENGINE TEXT,
			TABLE_COMMENT TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS "information_schema"."columns" (
			TABLE_CATALOG TEXT,
			TABLE_SCHEMA TEXT,
			TABLE_NAME TEXT,
			COLUMN_NAME TEXT,
			ORDINAL_POSITION INTEGER,
			COLUMN_DEFAULT TEXT,
			IS_NULLABLE TEXT,
			DATA_TYPE TEXT,
			COLUMN_TYPE TEXT,
			COLUMN_KEY TEXT,
			EXTRA TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS "information_schema"."key_column_usage" (
			CONSTRAINT_CATALOG TEXT,
			CONSTRAINT_SCHEMA TEXT,
			CONSTRAINT_NAME TEXT,
			TABLE_SCHEMA TEXT,
			TABLE_NAME TEXT,
			COLUMN_NAME TEXT,
			ORDINAL_POSITION INTEGER,
			POSITION_IN_UNIQUE_CONSTRAINT INTEGER,
			REFERENCED_TABLE_SCHEMA TEXT,
			REFERENCED_TABLE_NAME TEXT,
			REFERENCED_COLUMN_NAME TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS "information_schema"."statistics" (
			TABLE_CATALOG TEXT,
			TABLE_SCHEMA TEXT,
			TABLE_NAME TEXT,
			NON_UNIQUE INTEGER,
			INDEX_SCHEMA TEXT,
			INDEX_NAME TEXT,
			SEQ_IN_INDEX INTEGER,
			COLUMN_NAME TEXT,
			INDEX_TYPE TEXT,
			SUB_PART INTEGER,
			VISIBLE TEXT,
			EXPRESSION TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS "information_schema"."table_constraints" (
			CONSTRAINT_CATALOG TEXT,
			CONSTRAINT_SCHEMA TEXT,
			CONSTRAINT_NAME TEXT,
			TABLE_SCHEMA TEXT,
			TABLE_NAME TEXT,
			CONSTRAINT_TYPE TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS "information_schema"."referential_constraints" (
			CONSTRAINT_CATALOG TEXT,
			CONSTRAINT_SCHEMA TEXT,
			CONSTRAINT_NAME TEXT,
			UNIQUE_CONSTRAINT_CATALOG TEXT,
			UNIQUE_CONSTRAINT_SCHEMA TEXT,
			UNIQUE_CONSTRAINT_NAME TEXT,
			MATCH_OPTION TEXT,
			UPDATE_RULE TEXT,
			DELETE_RULE TEXT,
			TABLE_NAME TEXT,
			REFERENCED_TABLE_NAME TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS "information_schema"."check_constraints" (
			CONSTRAINT_CATALOG TEXT,
			CONSTRAINT_SCHEMA TEXT,
			CONSTRAINT_NAME TEXT,
			CHECK_CLAUSE TEXT
		)`,
	}
	for _, stmt := range statements {
		if _, err := c.sqliteConn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create information_schema table: %w", err)
		}
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaSchemata(ctx context.Context) error {
	_, err := c.sqliteConn.ExecContext(ctx, `
INSERT INTO "information_schema"."schemata"
  (CATALOG_NAME, SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME)
VALUES
  ('def', ?, ?, ?)`,
		c.currentDB, c.characterSetConnection, c.collationConnection)
	if err != nil {
		return fmt.Errorf("insert information_schema.schemata row for %s: %w", c.currentDB, err)
	}
	return nil
}

func (c *mysqlConn) refreshInformationSchemaSchemata(ctx context.Context) error {
	if _, err := c.sqliteConn.ExecContext(ctx, `DELETE FROM "information_schema"."schemata"`); err != nil {
		return fmt.Errorf("clear information_schema.schemata: %w", err)
	}
	return c.insertInformationSchemaSchemata(ctx)
}

func (c *mysqlConn) insertInformationSchemaTableMetadata(ctx context.Context, tableName, tableType, createSQL string) error {
	if err := c.insertInformationSchemaTable(ctx, tableName, tableType); err != nil {
		return err
	}
	if err := c.insertInformationSchemaColumns(ctx, tableName, createSQL); err != nil {
		return err
	}
	if err := c.insertInformationSchemaKeys(ctx, tableName); err != nil {
		return err
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaTable(ctx context.Context, tableName, sqliteType string) error {
	tableType := "BASE TABLE"
	if sqliteType == "view" {
		tableType = "VIEW"
	}
	_, err := c.sqliteConn.ExecContext(ctx, `
INSERT INTO "information_schema"."tables"
  (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_COMMENT)
VALUES
  ('def', ?, ?, ?, 'SQLite', '')`,
		c.currentDB, tableName, tableType)
	if err != nil {
		return fmt.Errorf("insert information_schema.tables row for %s: %w", tableName, err)
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaKeys(ctx context.Context, tableName string) error {
	if err := c.insertInformationSchemaPrimaryKeys(ctx, tableName); err != nil {
		return err
	}
	if err := c.insertInformationSchemaIndexes(ctx, tableName); err != nil {
		return err
	}
	if err := c.insertInformationSchemaForeignKeys(ctx, tableName); err != nil {
		return err
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaPrimaryKeys(ctx context.Context, tableName string) error {
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.table_info("+quoteIdent(tableName)+")")
	if err != nil {
		return fmt.Errorf("list sqlite primary keys for information_schema.%s: %w", tableName, err)
	}
	defer rows.Close()

	primaryKeyFound := false
	for rows.Next() {
		var cid int
		var columnName string
		var declaredType string
		var notNull int
		var defaultValue sql.NullString
		var primaryKey int
		if err := rows.Scan(&cid, &columnName, &declaredType, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("scan sqlite primary key for information_schema.%s: %w", tableName, err)
		}
		if primaryKey == 0 {
			continue
		}
		if !primaryKeyFound {
			if err := c.insertInformationSchemaTableConstraint(ctx, "PRIMARY", tableName, "PRIMARY KEY"); err != nil {
				return err
			}
			primaryKeyFound = true
		}
		if err := c.insertInformationSchemaKeyColumn(ctx, "PRIMARY", tableName, columnName, primaryKey, nil, "", ""); err != nil {
			return err
		}
		if err := c.insertInformationSchemaStatistic(ctx, tableName, "PRIMARY", 0, primaryKey, columnName); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list sqlite primary keys for information_schema.%s: %w", tableName, err)
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaIndexes(ctx context.Context, tableName string) error {
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.index_list("+quoteIdent(tableName)+")")
	if err != nil {
		return fmt.Errorf("list sqlite indexes for information_schema.%s: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var seq int
		var sqliteIndexName string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &sqliteIndexName, &unique, &origin, &partial); err != nil {
			return fmt.Errorf("scan sqlite index for information_schema.%s: %w", tableName, err)
		}
		_ = seq
		_ = origin
		_ = partial
		if sqliteIndexName == "" {
			continue
		}
		indexName := sqliteIndexName
		if metadata, ok := c.server.lookupMySQLIndexMetadataBySQLiteName(tableName, sqliteIndexName); ok {
			indexName = metadata.IndexName
		}
		if unique != 0 && origin != "pk" {
			if err := c.insertInformationSchemaTableConstraint(ctx, indexName, tableName, "UNIQUE"); err != nil {
				return err
			}
		}
		if err := c.insertInformationSchemaIndexColumns(ctx, tableName, sqliteIndexName, indexName, unique); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list sqlite indexes for information_schema.%s: %w", tableName, err)
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaIndexColumns(ctx context.Context, tableName, sqliteIndexName, indexName string, unique int) error {
	metadata, hasMetadata := c.server.lookupMySQLIndexMetadataBySQLiteName(tableName, sqliteIndexName)
	visible := "YES"
	if hasMetadata && metadata.Visible != "" {
		visible = metadata.Visible
	}
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.index_info("+quoteIdent(sqliteIndexName)+")")
	if err != nil {
		return fmt.Errorf("list sqlite index columns for information_schema.%s.%s: %w", tableName, indexName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var seqno int
		var cid int
		var columnName sql.NullString
		if err := rows.Scan(&seqno, &cid, &columnName); err != nil {
			return fmt.Errorf("scan sqlite index column for information_schema.%s.%s: %w", tableName, indexName, err)
		}
		columnMetadata := mysqlIndexColumnMetadata{}
		if hasMetadata && seqno < len(metadata.Columns) {
			columnMetadata = metadata.Columns[seqno]
		}
		if !columnName.Valid && columnMetadata.Expression == "" {
			continue
		}
		insertColumnName := columnName.String
		if columnMetadata.ColumnName != "" {
			insertColumnName = columnMetadata.ColumnName
		}
		position := seqno + 1
		if unique != 0 && insertColumnName != "" {
			if err := c.insertInformationSchemaKeyColumn(ctx, indexName, tableName, insertColumnName, position, nil, "", ""); err != nil {
				return err
			}
		}
		if err := c.insertInformationSchemaStatisticWithMetadata(ctx, tableName, indexName, boolInt(unique == 0), position, insertColumnName, columnMetadata.SubPart, visible, columnMetadata.Expression); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list sqlite index columns for information_schema.%s.%s: %w", tableName, indexName, err)
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaForeignKeys(ctx context.Context, tableName string) error {
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.foreign_key_list("+quoteIdent(tableName)+")")
	if err != nil {
		return fmt.Errorf("list sqlite foreign keys for information_schema.%s: %w", tableName, err)
	}
	defer rows.Close()

	insertedConstraints := map[int]bool{}
	for rows.Next() {
		var id int
		var seq int
		var refTable string
		var fromColumn string
		var toColumn string
		var onUpdate string
		var onDelete string
		var match string
		if err := rows.Scan(&id, &seq, &refTable, &fromColumn, &toColumn, &onUpdate, &onDelete, &match); err != nil {
			return fmt.Errorf("scan sqlite foreign key for information_schema.%s: %w", tableName, err)
		}
		position := seq + 1
		positionInUnique := position
		constraintName := fmt.Sprintf("fk_%s_%d", tableName, id)
		if !insertedConstraints[id] {
			if err := c.insertInformationSchemaTableConstraint(ctx, constraintName, tableName, "FOREIGN KEY"); err != nil {
				return err
			}
			if err := c.insertInformationSchemaReferentialConstraint(ctx, constraintName, tableName, refTable, onUpdate, onDelete, match); err != nil {
				return err
			}
			insertedConstraints[id] = true
		}
		if err := c.insertInformationSchemaKeyColumn(ctx, constraintName, tableName, fromColumn, position, &positionInUnique, refTable, toColumn); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list sqlite foreign keys for information_schema.%s: %w", tableName, err)
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaTableConstraint(ctx context.Context, constraintName, tableName, constraintType string) error {
	_, err := c.sqliteConn.ExecContext(ctx, `
INSERT INTO "information_schema"."table_constraints"
  (CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA,
   TABLE_NAME, CONSTRAINT_TYPE)
VALUES
  ('def', ?, ?, ?, ?, ?)`,
		c.currentDB, constraintName, c.currentDB, tableName, constraintType)
	if err != nil {
		return fmt.Errorf("insert information_schema.table_constraints row for %s.%s: %w", tableName, constraintName, err)
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaReferentialConstraint(ctx context.Context, constraintName, tableName, refTable, onUpdate, onDelete, match string) error {
	_, err := c.sqliteConn.ExecContext(ctx, `
INSERT INTO "information_schema"."referential_constraints"
  (CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME,
   UNIQUE_CONSTRAINT_CATALOG, UNIQUE_CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_NAME,
   MATCH_OPTION, UPDATE_RULE, DELETE_RULE, TABLE_NAME, REFERENCED_TABLE_NAME)
VALUES
  ('def', ?, ?, 'def', ?, 'PRIMARY', ?, ?, ?, ?, ?)`,
		c.currentDB, constraintName, c.currentDB, normalizeMatchOption(match),
		normalizeReferentialAction(onUpdate), normalizeReferentialAction(onDelete), tableName, refTable)
	if err != nil {
		return fmt.Errorf("insert information_schema.referential_constraints row for %s.%s: %w", tableName, constraintName, err)
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaKeyColumn(ctx context.Context, constraintName, tableName, columnName string, ordinalPosition int, positionInUnique *int, refTable, refColumn string) error {
	var positionArg any
	if positionInUnique != nil {
		positionArg = *positionInUnique
	}
	var refSchemaArg any
	var refTableArg any
	var refColumnArg any
	if refTable != "" {
		refSchemaArg = c.currentDB
		refTableArg = refTable
		refColumnArg = refColumn
	}
	_, err := c.sqliteConn.ExecContext(ctx, `
INSERT INTO "information_schema"."key_column_usage"
  (CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA,
   TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT,
   REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME)
VALUES
  ('def', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.currentDB, constraintName, c.currentDB, tableName, columnName, ordinalPosition,
		positionArg, refSchemaArg, refTableArg, refColumnArg)
	if err != nil {
		return fmt.Errorf("insert information_schema.key_column_usage row for %s.%s: %w", tableName, columnName, err)
	}
	return nil
}

func normalizeReferentialAction(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "NO ACTION"
	}
	return strings.ToUpper(value)
}

func normalizeMatchOption(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "NONE") {
		return "NONE"
	}
	return strings.ToUpper(value)
}

func (c *mysqlConn) insertInformationSchemaStatistic(ctx context.Context, tableName, indexName string, nonUnique, seqInIndex int, columnName string) error {
	return c.insertInformationSchemaStatisticWithMetadata(ctx, tableName, indexName, nonUnique, seqInIndex, columnName, nil, "YES", "")
}

func (c *mysqlConn) insertInformationSchemaStatisticWithMetadata(ctx context.Context, tableName, indexName string, nonUnique, seqInIndex int, columnName string, subPart *int, visible, expression string) error {
	var columnArg any
	if columnName != "" {
		columnArg = columnName
	}
	var subPartArg any
	if subPart != nil {
		subPartArg = *subPart
	}
	var expressionArg any
	if expression != "" {
		expressionArg = expression
	}
	if visible == "" {
		visible = "YES"
	}
	_, err := c.sqliteConn.ExecContext(ctx, `
INSERT INTO "information_schema"."statistics"
  (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, NON_UNIQUE, INDEX_SCHEMA,
   INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, INDEX_TYPE, SUB_PART, VISIBLE, EXPRESSION)
VALUES
  ('def', ?, ?, ?, ?, ?, ?, ?, 'BTREE', ?, ?, ?)`,
		c.currentDB, tableName, nonUnique, c.currentDB, indexName, seqInIndex, columnArg, subPartArg, visible, expressionArg)
	if err != nil {
		return fmt.Errorf("insert information_schema.statistics row for %s.%s: %w", tableName, columnName, err)
	}
	return nil
}

func (c *mysqlConn) insertInformationSchemaColumns(ctx context.Context, tableName, createSQL string) error {
	rows, err := c.sqliteConn.QueryContext(ctx, "PRAGMA main.table_info("+quoteIdent(tableName)+")")
	if err != nil {
		return fmt.Errorf("list sqlite columns for information_schema.%s: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var columnName string
		var declaredType string
		var notNull int
		var defaultValue sql.NullString
		var primaryKey int
		if err := rows.Scan(&cid, &columnName, &declaredType, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("scan sqlite column for information_schema.%s: %w", tableName, err)
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
		if primaryKey != 0 && strings.Contains(strings.ToUpper(createSQL), "AUTOINCREMENT") {
			extra = "auto_increment"
		}

		var defaultArg any
		if defaultValue.Valid {
			defaultArg = mysqlColumnDefaultMetadataValue(defaultValue.String)
		}
		_, err := c.sqliteConn.ExecContext(ctx, `
INSERT INTO "information_schema"."columns"
  (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION,
   COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY, EXTRA)
VALUES
  ('def', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			c.currentDB, tableName, columnName, cid+1, defaultArg, isNullable,
			dataType, columnType, columnKey, extra)
		if err != nil {
			return fmt.Errorf("insert information_schema.columns row for %s.%s: %w", tableName, columnName, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list sqlite columns for information_schema.%s: %w", tableName, err)
	}
	return nil
}

func mysqlColumnDefaultMetadataValue(defaultSQL string) any {
	value := strings.TrimSpace(defaultSQL)
	if strings.EqualFold(value, "NULL") {
		return nil
	}
	if unquoted, ok := unquoteSQLStringLiteral(value); ok {
		return unquoted
	}
	return value
}

func unquoteSQLStringLiteral(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) < 2 {
		return "", false
	}
	quote := value[0]
	if value[len(value)-1] != quote {
		return "", false
	}
	switch quote {
	case '\'':
		return strings.ReplaceAll(value[1:len(value)-1], "''", "'"), true
	case '"':
		return strings.ReplaceAll(value[1:len(value)-1], `""`, `"`), true
	default:
		return "", false
	}
}

func informationSchemaColumnTypes(declaredType string) (dataType, columnType string) {
	columnType = strings.ToLower(strings.TrimSpace(declaredType))
	if columnType == "" {
		return "", ""
	}
	dataType = columnType
	if idx := strings.IndexAny(dataType, " ("); idx >= 0 {
		dataType = dataType[:idx]
	}
	switch {
	case strings.Contains(dataType, "int"):
		dataType = "integer"
	case dataType == "real" || dataType == "float" || dataType == "double":
		dataType = "double"
	case dataType == "numeric" || dataType == "decimal":
		dataType = "decimal"
	case strings.Contains(dataType, "char") || dataType == "clob" || dataType == "text":
		dataType = "text"
	case strings.Contains(dataType, "blob"):
		dataType = "blob"
	}
	return dataType, columnType
}

func informationSchemaTargetTable(sqlText string, args []any) (string, bool) {
	if containsSQLIdentifier(sqlText, "OR") || informationSchemaReferencesSchemata(sqlText) {
		return "", false
	}
	for i := 0; i < len(sqlText); {
		if sqlText[i] == '\'' {
			end, _ := quotedSQLSpan(sqlText, i)
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			i = end
			continue
		}
		name, end, ok := readSQLNameToken(sqlText, i)
		if !ok {
			i++
			continue
		}
		if !strings.EqualFold(unquoteSQLWord(name), "TABLE_NAME") {
			i = end
			continue
		}
		next := skipSQLSpaces(sqlText, end)
		if next >= len(sqlText) || sqlText[next] != '=' {
			i = end
			continue
		}
		valueStart := skipSQLSpaces(sqlText, next+1)
		if valueStart >= len(sqlText) {
			return "", false
		}
		if sqlText[valueStart] == '?' {
			index := countPlaceholders(sqlText[:valueStart])
			return informationSchemaTableNameArg(args, index)
		}
		if sqlText[valueStart] == '\'' || sqlText[valueStart] == '"' {
			valueEnd, ok := quotedSQLSpan(sqlText, valueStart)
			if !ok {
				return "", false
			}
			return unquoteSQLWord(sqlText[valueStart:valueEnd]), true
		}
		return "", false
	}
	return "", false
}

func informationSchemaReferencesSchemata(sqlText string) bool {
	unquoted := strings.NewReplacer("`", "", `"`, "").Replace(strings.ToUpper(sqlText))
	normalized := strings.Join(strings.Fields(unquoted), " ")
	return strings.Contains(normalized, "INFORMATION_SCHEMA.SCHEMATA") ||
		strings.Contains(normalized, "INFORMATION_SCHEMA . SCHEMATA")
}

func informationSchemaTableNameArg(args []any, index int) (string, bool) {
	if index < 0 || index >= len(args) {
		return "", false
	}
	switch value := args[index].(type) {
	case string:
		return value, value != ""
	case []byte:
		return string(value), len(value) != 0
	default:
		return "", false
	}
}

func rewriteInformationSchemaSQL(sqlText, currentDB string) string {
	var out strings.Builder
	out.Grow(len(sqlText))

	for i := 0; i < len(sqlText); {
		if copyEnd, ok := quotedSQLSpan(sqlText, i); ok {
			out.WriteString(sqlText[i:copyEnd])
			i = copyEnd
			continue
		}
		if copyEnd, ok := sqlCommentSpan(sqlText, i); ok {
			out.WriteString(sqlText[i:copyEnd])
			i = copyEnd
			continue
		}
		if isSQLIdentifierStart(sqlText[i]) {
			start := i
			i++
			for i < len(sqlText) && isSQLIdentifierPart(sqlText[i]) {
				i++
			}
			ident := sqlText[start:i]
			upper := strings.ToUpper(ident)
			if upper == "DATABASE" || upper == "SCHEMA" {
				if next := consumeEmptyCall(sqlText, i); next >= 0 {
					out.WriteString(sqlStringLiteral(currentDB))
					i = next
					continue
				}
			}
			out.WriteString(ident)
			continue
		}
		out.WriteByte(sqlText[i])
		i++
	}
	return out.String()
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
