package mysqlmock

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

type mysqlIndexMetadata struct {
	TableName  string
	IndexName  string
	SQLiteName string
	Columns    []mysqlIndexColumnMetadata
	Visible    string
}

type mysqlIndexColumnMetadata struct {
	ColumnName string
	SubPart    *int
	Expression string
}

type mysqlColumnMetadata struct {
	TableName     string
	ColumnName    string
	ZeroFillWidth int
}

func (s *Server) recordMySQLColumnMetadata(sqlText string) {
	for _, metadata := range parseMySQLColumnMetadata(sqlText) {
		if metadata.TableName == "" || metadata.ColumnName == "" || metadata.ZeroFillWidth <= 0 {
			continue
		}
		s.mu.Lock()
		if s.columnMetadata == nil {
			s.columnMetadata = map[string]mysqlColumnMetadata{}
		}
		s.columnMetadata[columnMetadataKey(metadata.TableName, metadata.ColumnName)] = metadata
		s.mu.Unlock()
	}
}

func (s *Server) lookupMySQLColumnMetadata(tableName, columnName string) (mysqlColumnMetadata, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	metadata, ok := s.columnMetadata[columnMetadataKey(tableName, columnName)]
	return metadata, ok
}

func (s *Server) renameMySQLTableColumnMetadata(oldName, newName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	renamed := map[string]mysqlColumnMetadata{}
	for key, metadata := range s.columnMetadata {
		if !strings.EqualFold(metadata.TableName, oldName) {
			renamed[key] = metadata
			continue
		}
		metadata.TableName = newName
		renamed[columnMetadataKey(newName, metadata.ColumnName)] = metadata
	}
	s.columnMetadata = renamed
}

func (s *Server) recordMySQLIndexMetadata(sqlText string) {
	for _, metadata := range parseMySQLIndexMetadata(sqlText) {
		if metadata.TableName == "" || metadata.IndexName == "" {
			continue
		}
		if metadata.Visible == "" {
			metadata.Visible = "YES"
		}
		if metadata.SQLiteName == "" {
			metadata.SQLiteName = sqliteIndexName(metadata.TableName, metadata.IndexName)
		}
		s.mu.Lock()
		if s.indexMetadata == nil {
			s.indexMetadata = map[string]mysqlIndexMetadata{}
		}
		s.indexMetadata[indexMetadataKey(metadata.TableName, metadata.IndexName)] = metadata
		s.mu.Unlock()
	}
}

func (s *Server) renameMySQLIndexMetadata(tableName, oldName, newName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := indexMetadataKey(tableName, oldName)
	metadata, ok := s.indexMetadata[key]
	if !ok {
		return
	}
	delete(s.indexMetadata, key)
	metadata.IndexName = newName
	metadata.SQLiteName = sqliteIndexName(tableName, newName)
	s.indexMetadata[indexMetadataKey(tableName, newName)] = metadata
}

func (s *Server) dropMySQLIndexMetadata(tableName, indexName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.indexMetadata, indexMetadataKey(tableName, indexName))
}

func (s *Server) renameMySQLTableIndexMetadata(oldName, newName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	renamed := map[string]mysqlIndexMetadata{}
	for key, metadata := range s.indexMetadata {
		if !strings.EqualFold(metadata.TableName, oldName) {
			renamed[key] = metadata
			continue
		}
		metadata.TableName = newName
		renamed[indexMetadataKey(newName, metadata.IndexName)] = metadata
	}
	s.indexMetadata = renamed
}

func (s *Server) setMySQLIndexVisibility(tableName, indexName, visible string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := indexMetadataKey(tableName, indexName)
	metadata, ok := s.indexMetadata[key]
	if !ok {
		metadata = mysqlIndexMetadata{
			TableName:  tableName,
			IndexName:  indexName,
			SQLiteName: sqliteIndexName(tableName, indexName),
		}
	}
	metadata.Visible = visible
	s.indexMetadata[key] = metadata
}

func (s *Server) lookupMySQLIndexMetadata(tableName, indexName string) (mysqlIndexMetadata, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	metadata, ok := s.indexMetadata[indexMetadataKey(tableName, indexName)]
	return metadata, ok
}

func (s *Server) lookupMySQLIndexMetadataBySQLiteName(tableName, sqliteName string) (mysqlIndexMetadata, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, metadata := range s.indexMetadata {
		if !strings.EqualFold(metadata.TableName, tableName) {
			continue
		}
		if metadata.SQLiteName == "" {
			metadata.SQLiteName = sqliteIndexName(metadata.TableName, metadata.IndexName)
		}
		if strings.EqualFold(metadata.SQLiteName, sqliteName) {
			return metadata, true
		}
	}
	return mysqlIndexMetadata{}, false
}

func (s *Server) sqliteIndexNameForMySQL(tableName, indexName string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if metadata, ok := s.indexMetadata[indexMetadataKey(tableName, indexName)]; ok && metadata.SQLiteName != "" {
		return metadata.SQLiteName
	}
	return sqliteIndexName(tableName, indexName)
}

func (s *Server) recordMySQLTableDDL(sqlText string) {
	tableName, ok := parseCreateTableDDLTableName(sqlText)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tableDDL == nil {
		s.tableDDL = map[string]string{}
	}
	s.tableDDL[tableMetadataKey(tableName)] = normalizeShowCreateTableDDL(sqlText)
}

func (s *Server) lookupMySQLTableDDL(tableName string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ddl, ok := s.tableDDL[tableMetadataKey(tableName)]
	return ddl, ok
}

func (s *Server) invalidateMySQLTableDDL(tableName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tableDDL, tableMetadataKey(tableName))
}

func (s *Server) renameMySQLTableDDL(oldName, newName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tableMetadataKey(oldName)
	ddl, ok := s.tableDDL[key]
	if !ok {
		return
	}
	delete(s.tableDDL, key)
	s.tableDDL[tableMetadataKey(newName)] = ddl
}

func (s *Server) invalidateMySQLTableDDLForStatement(sqlText string) {
	if tableName, ok := parseCreateTableDDLTableName(sqlText); ok {
		_ = tableName
		return
	}
	if tableName, ok := parseDropTableName(sqlText); ok {
		s.invalidateMySQLTableDDL(tableName)
		return
	}
	if tableName, ok := parseAlterTableName(sqlText); ok {
		s.invalidateMySQLTableDDL(tableName)
	}
}

func indexMetadataKey(tableName, indexName string) string {
	return strings.ToLower(unquoteSQLWord(tableName)) + "." + strings.ToLower(unquoteSQLWord(indexName))
}

func columnMetadataKey(tableName, columnName string) string {
	return strings.ToLower(unquoteSQLWord(tableName)) + "." + strings.ToLower(unquoteSQLWord(columnName))
}

func tableMetadataKey(tableName string) string {
	return strings.ToLower(unquoteSQLWord(tableName))
}

func sqliteIndexName(tableName, indexName string) string {
	tableName = unquoteSQLWord(tableName)
	indexName = unquoteSQLWord(indexName)

	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(tableName)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.ToLower(indexName)))

	return fmt.Sprintf("__mysqlmock_idx_%s_%s_%08x",
		sanitizeSQLName(tableName),
		sanitizeSQLName(indexName),
		h.Sum32())
}

func parseCreateTableDDLTableName(sqlText string) (string, bool) {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "CREATE") {
		return "", false
	}
	_ = consumeKeyword(sqlText, &pos, "TEMPORARY")
	_ = consumeKeyword(sqlText, &pos, "TEMP")
	if !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", false
	}
	if consumeKeyword(sqlText, &pos, "IF") {
		if !consumeKeyword(sqlText, &pos, "NOT") || !consumeKeyword(sqlText, &pos, "EXISTS") {
			return "", false
		}
	}
	tableName, _, ok := readSQLQualifiedName(sqlText, pos)
	return tableName, ok
}

func parseDropTableName(sqlText string) (string, bool) {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "DROP") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", false
	}
	if consumeKeyword(sqlText, &pos, "IF") {
		if !consumeKeyword(sqlText, &pos, "EXISTS") {
			return "", false
		}
	}
	tableName, _, ok := readSQLQualifiedName(sqlText, pos)
	return tableName, ok
}

func parseAlterTableName(sqlText string) (string, bool) {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "ALTER") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return "", false
	}
	tableName, _, ok := readSQLQualifiedName(sqlText, pos)
	return tableName, ok
}

func normalizeShowCreateTableDDL(sqlText string) string {
	ddl := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sqlText), ";"))
	if !strings.Contains(strings.ToUpper(ddl), " ENGINE=") {
		ddl += " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
	}
	return ddl
}

func parseMySQLIndexMetadata(sqlText string) []mysqlIndexMetadata {
	if metadata, ok := parseCreateIndexMetadata(sqlText); ok {
		return []mysqlIndexMetadata{metadata}
	}
	if metadata, ok := parseAlterTableAddIndexMetadata(sqlText); ok {
		return []mysqlIndexMetadata{metadata}
	}
	return parseCreateTableIndexMetadata(sqlText)
}

func parseMySQLColumnMetadata(sqlText string) []mysqlColumnMetadata {
	tableName, ok := parseCreateTableDDLTableName(sqlText)
	if !ok {
		return nil
	}
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "CREATE") {
		return nil
	}
	_ = consumeKeyword(sqlText, &pos, "TEMPORARY")
	_ = consumeKeyword(sqlText, &pos, "TEMP")
	if !consumeKeyword(sqlText, &pos, "TABLE") {
		return nil
	}
	if consumeKeyword(sqlText, &pos, "IF") {
		if !consumeKeyword(sqlText, &pos, "NOT") || !consumeKeyword(sqlText, &pos, "EXISTS") {
			return nil
		}
	}
	if _, next, ok := readSQLQualifiedName(sqlText, pos); ok {
		pos = next
	}
	bodyStart := skipSQLSpaces(sqlText, pos)
	bodyEnd, ok := parenthesizedSQLSpan(sqlText, bodyStart)
	if !ok {
		return nil
	}
	metadata := []mysqlColumnMetadata{}
	for _, item := range splitSQLTopLevelList(sqlText[bodyStart+1 : bodyEnd-1]) {
		item = strings.TrimSpace(item)
		columnName, pos, ok := readSQLNameToken(item, 0)
		if !ok || isCreateTableConstraintItem(columnName) {
			continue
		}
		if width := mysqlZeroFillWidth(item[pos:]); width > 0 {
			metadata = append(metadata, mysqlColumnMetadata{
				TableName:     tableName,
				ColumnName:    unquoteSQLWord(columnName),
				ZeroFillWidth: width,
			})
		}
	}
	return metadata
}

func isCreateTableConstraintItem(firstToken string) bool {
	switch strings.ToUpper(unquoteSQLWord(firstToken)) {
	case "PRIMARY", "UNIQUE", "KEY", "INDEX", "CONSTRAINT", "FOREIGN", "CHECK", "FULLTEXT", "SPATIAL":
		return true
	default:
		return false
	}
}

func parseCreateIndexMetadata(sqlText string) (mysqlIndexMetadata, bool) {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "CREATE") {
		return mysqlIndexMetadata{}, false
	}
	_ = consumeKeyword(sqlText, &pos, "UNIQUE")
	if !consumeKeyword(sqlText, &pos, "INDEX") {
		return mysqlIndexMetadata{}, false
	}
	indexName, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return mysqlIndexMetadata{}, false
	}
	if next, ok := consumeSQLNamedOption(sqlText, pos, "USING"); ok {
		pos = next
	}
	if !consumeKeyword(sqlText, &pos, "ON") {
		return mysqlIndexMetadata{}, false
	}
	tableName, pos, ok := readSQLQualifiedName(sqlText, pos)
	if !ok {
		return mysqlIndexMetadata{}, false
	}
	columnsStart := skipSQLSpaces(sqlText, pos)
	columnsEnd, ok := parenthesizedSQLSpan(sqlText, columnsStart)
	if !ok {
		return mysqlIndexMetadata{}, false
	}
	return mysqlIndexMetadata{
		TableName: tableName,
		IndexName: unquoteSQLWord(indexName),
		Columns:   parseMySQLIndexColumnMetadata(sqlText[columnsStart:columnsEnd]),
		Visible:   parseMySQLIndexVisibility(sqlText[columnsEnd:]),
	}, true
}

func parseAlterTableAddIndexMetadata(sqlText string) (mysqlIndexMetadata, bool) {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "ALTER") || !consumeKeyword(sqlText, &pos, "TABLE") {
		return mysqlIndexMetadata{}, false
	}
	tableName, pos, ok := readSQLQualifiedName(sqlText, pos)
	if !ok || !consumeKeyword(sqlText, &pos, "ADD") {
		return mysqlIndexMetadata{}, false
	}
	_ = consumeKeyword(sqlText, &pos, "UNIQUE")
	if !consumeKeyword(sqlText, &pos, "INDEX") && !consumeKeyword(sqlText, &pos, "KEY") {
		return mysqlIndexMetadata{}, false
	}
	if next, ok := consumeSQLNamedOption(sqlText, pos, "USING"); ok {
		pos = next
	}
	indexName, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return mysqlIndexMetadata{}, false
	}
	if next, ok := consumeSQLNamedOption(sqlText, pos, "USING"); ok {
		pos = next
	}
	columnsStart := skipSQLSpaces(sqlText, pos)
	columnsEnd, ok := parenthesizedSQLSpan(sqlText, columnsStart)
	if !ok {
		return mysqlIndexMetadata{}, false
	}
	return mysqlIndexMetadata{
		TableName: tableName,
		IndexName: unquoteSQLWord(indexName),
		Columns:   parseMySQLIndexColumnMetadata(sqlText[columnsStart:columnsEnd]),
		Visible:   parseMySQLIndexVisibility(sqlText[columnsEnd:]),
	}, true
}

func parseCreateTableIndexMetadata(sqlText string) []mysqlIndexMetadata {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "CREATE") {
		return nil
	}
	_ = consumeKeyword(sqlText, &pos, "TEMPORARY")
	_ = consumeKeyword(sqlText, &pos, "TEMP")
	if !consumeKeyword(sqlText, &pos, "TABLE") {
		return nil
	}
	if consumeKeyword(sqlText, &pos, "IF") {
		if !consumeKeyword(sqlText, &pos, "NOT") || !consumeKeyword(sqlText, &pos, "EXISTS") {
			return nil
		}
	}
	tableName, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return nil
	}
	bodyStart := skipSQLSpaces(sqlText, pos)
	bodyEnd, ok := parenthesizedSQLSpan(sqlText, bodyStart)
	if !ok {
		return nil
	}

	var indexes []mysqlIndexMetadata
	for _, item := range splitSQLTopLevelList(sqlText[bodyStart+1 : bodyEnd-1]) {
		if metadata, ok := parseCreateTableItemIndexMetadata(tableName, item); ok {
			indexes = append(indexes, metadata)
		}
	}
	return indexes
}

func parseCreateTableItemIndexMetadata(tableName, item string) (mysqlIndexMetadata, bool) {
	pos := 0
	item = strings.TrimSpace(item)
	constraintName := ""
	if consumeKeyword(item, &pos, "CONSTRAINT") {
		if name, next, ok := readSQLNameToken(item, pos); ok {
			constraintName = unquoteSQLWord(name)
			pos = next
		} else {
			return mysqlIndexMetadata{}, false
		}
	}
	unique := consumeKeyword(item, &pos, "UNIQUE")
	if !consumeKeyword(item, &pos, "KEY") && !consumeKeyword(item, &pos, "INDEX") {
		return mysqlIndexMetadata{}, false
	}
	if next, ok := consumeSQLNamedOption(item, pos, "USING"); ok {
		pos = next
	}
	indexName := constraintName
	columnsStart := skipSQLSpaces(item, pos)
	if columnsStart >= len(item) || item[columnsStart] != '(' {
		name, next, ok := readSQLNameToken(item, pos)
		if !ok {
			return mysqlIndexMetadata{}, false
		}
		indexName = unquoteSQLWord(name)
		pos = next
		if next, ok := consumeSQLNamedOption(item, pos, "USING"); ok {
			pos = next
		}
		columnsStart = skipSQLSpaces(item, pos)
	}
	columnsEnd, ok := parenthesizedSQLSpan(item, columnsStart)
	if !ok {
		return mysqlIndexMetadata{}, false
	}
	if indexName == "" {
		indexName = generatedIndexName(tableName, item[columnsStart:columnsEnd], unique)
	}
	return mysqlIndexMetadata{
		TableName: unquoteSQLWord(tableName),
		IndexName: indexName,
		Columns:   parseMySQLIndexColumnMetadata(item[columnsStart:columnsEnd]),
		Visible:   parseMySQLIndexVisibility(item[columnsEnd:]),
	}, true
}

func parseMySQLIndexColumnMetadata(columns string) []mysqlIndexColumnMetadata {
	if len(columns) < 2 {
		return nil
	}
	items := splitSQLTopLevelList(columns[1 : len(columns)-1])
	out := make([]mysqlIndexColumnMetadata, 0, len(items))
	for _, item := range items {
		out = append(out, parseMySQLIndexColumnItemMetadata(item))
	}
	return out
}

func parseMySQLIndexColumnItemMetadata(item string) mysqlIndexColumnMetadata {
	item = trimIndexColumnOrder(strings.TrimSpace(item))
	if item == "" {
		return mysqlIndexColumnMetadata{}
	}
	name, pos, ok := readSQLNameToken(item, 0)
	if ok {
		next := skipSQLSpaces(item, pos)
		if end, hasPrefix := parenthesizedSQLSpan(item, next); hasPrefix && isNumericParenthesized(item[next:end]) {
			length, _ := strconv.Atoi(strings.TrimSpace(item[next+1 : end-1]))
			return mysqlIndexColumnMetadata{ColumnName: unquoteSQLWord(name), SubPart: &length}
		}
		if next == len(item) {
			return mysqlIndexColumnMetadata{ColumnName: unquoteSQLWord(name)}
		}
	}
	if end, ok := parenthesizedSQLSpan(item, 0); ok && skipSQLSpaces(item, end) == len(item) {
		return mysqlIndexColumnMetadata{Expression: strings.TrimSpace(item[1 : end-1])}
	}
	return mysqlIndexColumnMetadata{Expression: item}
}

func trimIndexColumnOrder(item string) string {
	fields := strings.Fields(item)
	if len(fields) == 0 {
		return item
	}
	last := strings.ToUpper(fields[len(fields)-1])
	if last != "ASC" && last != "DESC" {
		return item
	}
	return strings.TrimSpace(item[:strings.LastIndex(item, fields[len(fields)-1])])
}

func parseMySQLIndexVisibility(sqlText string) string {
	switch {
	case containsSQLIdentifier(sqlText, "INVISIBLE"):
		return "NO"
	case containsSQLIdentifier(sqlText, "VISIBLE"):
		return "YES"
	default:
		return "YES"
	}
}
