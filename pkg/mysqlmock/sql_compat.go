package mysqlmock

import (
	"fmt"
	"strings"
)

func translateSQLStatements(sqlText string) []string {
	if translated, ok := translateCreateTableStatements(sqlText); ok {
		return translated
	}
	return []string{translateSQL(sqlText)}
}

func translateCreateTableStatements(sqlText string) ([]string, bool) {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "CREATE") {
		return nil, false
	}
	if consumeKeyword(sqlText, &pos, "TEMPORARY") {
		// Keep the original header text; only advance the parser.
	} else {
		_ = consumeKeyword(sqlText, &pos, "TEMP")
	}
	if !consumeKeyword(sqlText, &pos, "TABLE") {
		return nil, false
	}
	if consumeKeyword(sqlText, &pos, "IF") {
		if !consumeKeyword(sqlText, &pos, "NOT") || !consumeKeyword(sqlText, &pos, "EXISTS") {
			return nil, false
		}
	}
	tableName, pos, ok := readSQLNameToken(sqlText, pos)
	if !ok {
		return nil, false
	}

	bodyStart := skipSQLSpaces(sqlText, pos)
	bodyEnd, ok := parenthesizedSQLSpan(sqlText, bodyStart)
	if !ok {
		return nil, false
	}

	items := splitSQLTopLevelList(sqlText[bodyStart+1 : bodyEnd-1])
	translatedItems := make([]string, 0, len(items))
	indexStatements := []string{}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if primary, ok := translatePrimaryKeyTableConstraint(item); ok {
			translatedItems = append(translatedItems, primary)
			continue
		}
		if indexStmt, ok := translateCreateTableIndex(item, tableName); ok {
			if indexStmt != "" {
				indexStatements = append(indexStatements, indexStmt)
			}
			continue
		}
		translatedItems = append(translatedItems, translateCreateTableColumnItem(item))
	}

	rebuilt := sqlText[:bodyStart+1] + strings.Join(translatedItems, ",\n") + sqlText[bodyEnd-1:]
	statements := []string{translateSQL(rebuilt)}
	statements = append(statements, indexStatements...)
	return statements, true
}

func translateCreateTableColumnItem(item string) string {
	item = expandTiDBDDLComments(item)
	if containsSQLIdentifier(item, "AUTO_RANDOM") {
		item = removeSQLIdentifierCall(item, "AUTO_RANDOM")
		item = replaceAutoRandomIntegerType(item)
		item = ensurePrimaryKeyAutoincrement(item)
	}
	return translateSQL(item)
}

func translatePrimaryKeyTableConstraint(item string) (string, bool) {
	pos := skipSQLSpaces(item, 0)
	if consumeKeyword(item, &pos, "CONSTRAINT") {
		if _, next, ok := readSQLNameToken(item, pos); ok {
			pos = next
		} else {
			return "", false
		}
	}
	if !consumeKeyword(item, &pos, "PRIMARY") || !consumeKeyword(item, &pos, "KEY") {
		return "", false
	}
	if next, ok := consumeSQLNamedOption(item, pos, "USING"); ok {
		pos = next
	}
	columnsStart := skipSQLSpaces(item, pos)
	columnsEnd, ok := parenthesizedSQLSpan(item, columnsStart)
	if !ok {
		return "", false
	}
	return translateSQL(item[:columnsEnd]), true
}

func translateCreateTableIndex(item, tableName string) (string, bool) {
	pos := skipSQLSpaces(item, 0)
	constraintName := ""
	if consumeKeyword(item, &pos, "CONSTRAINT") {
		name, next, ok := readSQLNameToken(item, pos)
		if !ok {
			return "", false
		}
		constraintName = name
		pos = next
	}

	unique := consumeKeyword(item, &pos, "UNIQUE")
	if !consumeKeyword(item, &pos, "KEY") && !consumeKeyword(item, &pos, "INDEX") {
		return "", false
	}
	if next, ok := consumeSQLNamedOption(item, pos, "USING"); ok {
		pos = next
	}

	indexName := constraintName
	columnsStart := skipSQLSpaces(item, pos)
	if columnsStart >= len(item) || item[columnsStart] != '(' {
		name, next, ok := readSQLNameToken(item, pos)
		if !ok {
			return "", false
		}
		indexName = name
		pos = next
		if next, ok := consumeSQLNamedOption(item, pos, "USING"); ok {
			pos = next
		}
		columnsStart = skipSQLSpaces(item, pos)
	}
	columnsEnd, ok := parenthesizedSQLSpan(item, columnsStart)
	if !ok {
		return "", false
	}
	if indexName == "" {
		indexName = quoteIdent(generatedIndexName(tableName, item[columnsStart:columnsEnd], unique))
	}

	var out strings.Builder
	out.WriteString("CREATE ")
	if unique {
		out.WriteString("UNIQUE ")
	}
	out.WriteString("INDEX ")
	out.WriteString(indexName)
	out.WriteString(" ON ")
	out.WriteString(tableName)
	out.WriteByte(' ')
	out.WriteString(translateMySQLIndexColumns(item[columnsStart:columnsEnd]))
	return out.String(), true
}

func translateMySQLIndexColumns(columns string) string {
	if len(columns) < 2 {
		return columns
	}
	items := splitSQLTopLevelList(columns[1 : len(columns)-1])
	for i, item := range items {
		items[i] = strings.TrimSpace(stripMySQLIndexPrefixLength(item))
	}
	return "(" + strings.Join(items, ", ") + ")"
}

func stripMySQLIndexPrefixLength(item string) string {
	name, pos, ok := readSQLNameToken(item, 0)
	if !ok {
		return item
	}
	next := skipSQLSpaces(item, pos)
	end, ok := parenthesizedSQLSpan(item, next)
	if !ok || !isNumericParenthesized(item[next:end]) {
		return item
	}
	return name + item[end:]
}

func isNumericParenthesized(value string) bool {
	if len(value) < 2 || value[0] != '(' || value[len(value)-1] != ')' {
		return false
	}
	inner := strings.TrimSpace(value[1 : len(value)-1])
	if inner == "" {
		return false
	}
	for i := 0; i < len(inner); i++ {
		if inner[i] < '0' || inner[i] > '9' {
			return false
		}
	}
	return true
}

func generatedIndexName(tableName, columns string, unique bool) string {
	parts := []string{"idx", sanitizeSQLName(tableName)}
	if unique {
		parts = append(parts, "uniq")
	}
	for _, item := range splitSQLTopLevelList(strings.Trim(columns, "()")) {
		name, _, ok := readSQLNameToken(strings.TrimSpace(item), 0)
		if !ok {
			continue
		}
		parts = append(parts, sanitizeSQLName(name))
	}
	return strings.Join(parts, "_")
}

func sanitizeSQLName(value string) string {
	value = unquoteSQLWord(value)
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ('0' <= ch && ch <= '9') {
			out.WriteByte(ch)
			continue
		}
		out.WriteByte('_')
	}
	result := strings.Trim(out.String(), "_")
	if result == "" {
		return "index"
	}
	return result
}

func splitSQLTopLevelList(sqlText string) []string {
	items := []string{}
	start := 0
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
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				items = append(items, sqlText[start:i])
				start = i + 1
			}
		}
		i++
	}
	items = append(items, sqlText[start:])
	return items
}

func expandTiDBDDLComments(sqlText string) string {
	var out strings.Builder
	out.Grow(len(sqlText))
	for i := 0; i < len(sqlText); {
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			if replacement, handled := translateTiDBDDLComment(sqlText[i:end]); handled {
				out.WriteString(replacement)
			} else {
				out.WriteString(sqlText[i:end])
			}
			i = end
			continue
		}
		out.WriteByte(sqlText[i])
		i++
	}
	return out.String()
}

func translateTiDBDDLComment(comment string) (string, bool) {
	trimmed := strings.TrimSpace(comment)
	if !strings.HasPrefix(trimmed, "/*T![") || !strings.HasSuffix(trimmed, "*/") {
		return "", false
	}
	closeTag := strings.Index(trimmed, "]")
	if closeTag < 0 {
		return "", false
	}
	tag := strings.ToLower(strings.TrimSpace(trimmed[len("/*T!["):closeTag]))
	body := strings.TrimSpace(trimmed[closeTag+1 : len(trimmed)-len("*/")])
	switch tag {
	case "auto_rand":
		if body == "" {
			body = "AUTO_RANDOM"
		}
		return " " + body + " ", true
	case "clustered_index":
		return "", true
	default:
		return "", false
	}
}

func containsSQLIdentifier(sqlText, name string) bool {
	for i := 0; i < len(sqlText); {
		if end, ok := quotedSQLSpan(sqlText, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			i = end
			continue
		}
		if ident, end, ok := readSQLIdentifier(sqlText, i); ok {
			if strings.EqualFold(ident, name) {
				return true
			}
			i = end
			continue
		}
		i++
	}
	return false
}

func removeSQLIdentifierCall(sqlText, name string) string {
	var out strings.Builder
	out.Grow(len(sqlText))
	for i := 0; i < len(sqlText); {
		if end, ok := quotedSQLSpan(sqlText, i); ok {
			out.WriteString(sqlText[i:end])
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			out.WriteString(sqlText[i:end])
			i = end
			continue
		}
		if ident, end, ok := readSQLIdentifier(sqlText, i); ok {
			if strings.EqualFold(ident, name) {
				if callEnd := consumeOptionalParenthesizedCall(sqlText, end); callEnd >= 0 {
					i = callEnd
				} else {
					i = end
				}
				continue
			}
			out.WriteString(sqlText[i:end])
			i = end
			continue
		}
		out.WriteByte(sqlText[i])
		i++
	}
	return out.String()
}

func replaceAutoRandomIntegerType(item string) string {
	_, pos, ok := readSQLNameToken(item, 0)
	if !ok {
		return item
	}
	typeStart := skipSQLSpaces(item, pos)
	typeName, typeEnd, ok := readSQLIdentifier(item, typeStart)
	if !ok || !strings.EqualFold(typeName, "BIGINT") {
		return item
	}
	return item[:typeStart] + "INTEGER" + item[typeEnd:]
}

func ensurePrimaryKeyAutoincrement(item string) string {
	if containsSQLIdentifier(item, "AUTOINCREMENT") {
		return item
	}
	for i := 0; i < len(item); {
		if end, ok := quotedSQLSpan(item, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(item, i); ok {
			i = end
			continue
		}
		word, end, ok := readSQLIdentifier(item, i)
		if !ok {
			i++
			continue
		}
		if !strings.EqualFold(word, "PRIMARY") {
			i = end
			continue
		}
		keyStart := skipSQLSpaces(item, end)
		key, keyEnd, ok := readSQLIdentifier(item, keyStart)
		if ok && strings.EqualFold(key, "KEY") {
			return item[:keyEnd] + " AUTOINCREMENT" + item[keyEnd:]
		}
		i = end
	}
	return item
}

func consumeOptionalParenthesizedCall(sqlText string, pos int) int {
	i := skipSQLSpaces(sqlText, pos)
	if i >= len(sqlText) || sqlText[i] != '(' {
		return -1
	}
	end, ok := parenthesizedSQLSpan(sqlText, i)
	if !ok {
		return -1
	}
	return end
}

func stripMySQLLockingClause(sqlText string) (string, bool) {
	for i := 0; i < len(sqlText); {
		if end, ok := quotedSQLSpan(sqlText, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			i = end
			continue
		}
		if sqlText[i] == '(' {
			end, ok := parenthesizedSQLSpan(sqlText, i)
			if ok {
				i = end
				continue
			}
		}
		word, end, ok := readSQLIdentifier(sqlText, i)
		if !ok {
			i++
			continue
		}
		if strings.EqualFold(word, "FOR") {
			next := skipSQLSpaces(sqlText, end)
			nextWord, _, ok := readSQLIdentifier(sqlText, next)
			if ok && (strings.EqualFold(nextWord, "UPDATE") || strings.EqualFold(nextWord, "SHARE")) {
				return trimSQLClauseSuffix(sqlText, i), true
			}
		}
		if strings.EqualFold(word, "LOCK") {
			pos := end
			if consumeKeyword(sqlText, &pos, "IN") && consumeKeyword(sqlText, &pos, "SHARE") && consumeKeyword(sqlText, &pos, "MODE") {
				return trimSQLClauseSuffix(sqlText, i), true
			}
		}
		i = end
	}
	return sqlText, false
}

func trimSQLClauseSuffix(sqlText string, start int) string {
	end := len(sqlText)
	for end > start && isSQLSpace(sqlText[end-1]) {
		end--
	}
	semicolon := ""
	if end > start && sqlText[end-1] == ';' {
		semicolon = ";"
	}
	return strings.TrimRightFunc(sqlText[:start], func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\f'
	}) + semicolon
}

func schemaStatementsFromDump(sqlText string) []string {
	statements := []string{}
	for _, stmt := range splitSQLStatements(removeSQLDumpDelimiterLines(sqlText)) {
		if !isSchemaDumpStatement(stmt) {
			continue
		}
		statements = append(statements, stmt)
	}
	return statements
}

func splitSQLStatements(sqlText string) []string {
	statements := []string{}
	start := 0
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
		case ')':
			if depth > 0 {
				depth--
			}
		case ';':
			if depth == 0 {
				stmt := strings.TrimSpace(sqlText[start:i])
				if stmt != "" {
					statements = append(statements, stmt)
				}
				start = i + 1
			}
		}
		i++
	}
	if stmt := strings.TrimSpace(sqlText[start:]); stmt != "" {
		statements = append(statements, stmt)
	}
	return statements
}

func removeSQLDumpDelimiterLines(sqlText string) string {
	lines := strings.Split(sqlText, "\n")
	out := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(line)), "DELIMITER ") {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func isSchemaDumpStatement(stmt string) bool {
	normalized := strings.ToUpper(normalizeSQL(stmt))
	switch {
	case strings.HasPrefix(normalized, "CREATE TABLE "),
		strings.HasPrefix(normalized, "CREATE TEMPORARY TABLE "),
		strings.HasPrefix(normalized, "CREATE TEMP TABLE "),
		strings.HasPrefix(normalized, "CREATE INDEX "),
		strings.HasPrefix(normalized, "CREATE UNIQUE INDEX "),
		strings.HasPrefix(normalized, "ALTER TABLE "),
		strings.HasPrefix(normalized, "DROP TABLE "),
		strings.HasPrefix(normalized, "DROP INDEX "):
		return true
	default:
		return false
	}
}

func sqlCompatErrorf(format string, args ...any) error {
	return errPacket(mysqlErrUnknown, "HY000", fmt.Sprintf(format, args...))
}
