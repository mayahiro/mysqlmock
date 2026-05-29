package mysqlmock

import "strings"

func normalizeSQL(sqlText string) string {
	trimmed := strings.TrimSpace(sqlText)
	trimmed = strings.TrimSuffix(trimmed, ";")
	return strings.Join(strings.Fields(trimmed), " ")
}

func unquoteSQLWord(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, ";")
	if len(value) >= 2 {
		quote := value[0]
		if (quote == '\'' || quote == '"' || quote == '`') && value[len(value)-1] == quote {
			return value[1 : len(value)-1]
		}
	}
	return value
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

func findTopLevelSQLKeyword(sqlText string, pos int, keyword string) (int, bool) {
	depth := 0
	for i := pos; i < len(sqlText); {
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
		word, end, ok := readSQLIdentifier(sqlText, i)
		if !ok {
			i++
			continue
		}
		if depth == 0 && strings.EqualFold(word, keyword) {
			return i, true
		}
		i = end
	}
	return 0, false
}

func consumeKeyword(sqlText string, pos *int, keyword string) bool {
	word, next, ok := readSQLIdentifier(sqlText, skipSQLSpaces(sqlText, *pos))
	if !ok || !strings.EqualFold(word, keyword) {
		return false
	}
	*pos = next
	return true
}

func readSQLNameToken(sqlText string, pos int) (string, int, bool) {
	pos = skipSQLSpaces(sqlText, pos)
	if pos >= len(sqlText) {
		return "", pos, false
	}
	if sqlText[pos] == '`' || sqlText[pos] == '"' {
		end, ok := quotedSQLSpan(sqlText, pos)
		if !ok {
			return "", pos, false
		}
		return sqlText[pos:end], end, true
	}
	if _, end, ok := readSQLIdentifier(sqlText, pos); ok {
		return sqlText[pos:end], end, true
	}
	return "", pos, false
}

func parenthesizedSQLSpan(sqlText string, pos int) (int, bool) {
	if pos >= len(sqlText) || sqlText[pos] != '(' {
		return pos, false
	}
	depth := 0
	for i := pos; i < len(sqlText); {
		if copyEnd, ok := quotedSQLSpan(sqlText, i); ok {
			i = copyEnd
			continue
		}
		if copyEnd, ok := sqlCommentSpan(sqlText, i); ok {
			i = copyEnd
			continue
		}
		switch sqlText[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1, true
			}
		}
		i++
	}
	return len(sqlText), false
}

func readSQLIdentifier(sqlText string, pos int) (string, int, bool) {
	if pos >= len(sqlText) || !isSQLIdentifierStart(sqlText[pos]) {
		return "", pos, false
	}
	start := pos
	pos++
	for pos < len(sqlText) && isSQLIdentifierPart(sqlText[pos]) {
		pos++
	}
	return sqlText[start:pos], pos, true
}

func skipSQLSpaces(sqlText string, pos int) int {
	for pos < len(sqlText) && isSQLSpace(sqlText[pos]) {
		pos++
	}
	return pos
}

func quotedSQLSpan(sqlText string, pos int) (int, bool) {
	if pos >= len(sqlText) {
		return pos, false
	}
	quote := sqlText[pos]
	if quote != '\'' && quote != '"' && quote != '`' {
		return pos, false
	}
	i := pos + 1
	for i < len(sqlText) {
		if sqlText[i] == '\\' && quote != '`' && i+1 < len(sqlText) {
			i += 2
			continue
		}
		if sqlText[i] == quote {
			if quote != '`' && i+1 < len(sqlText) && sqlText[i+1] == quote {
				i += 2
				continue
			}
			return i + 1, true
		}
		i++
	}
	return len(sqlText), true
}

func sqlCommentSpan(sqlText string, pos int) (int, bool) {
	if pos+1 < len(sqlText) && sqlText[pos] == '-' && sqlText[pos+1] == '-' {
		i := pos + 2
		for i < len(sqlText) && sqlText[i] != '\n' {
			i++
		}
		if i < len(sqlText) {
			i++
		}
		return i, true
	}
	if sqlText[pos] == '#' {
		i := pos + 1
		for i < len(sqlText) && sqlText[i] != '\n' {
			i++
		}
		if i < len(sqlText) {
			i++
		}
		return i, true
	}
	if pos+1 < len(sqlText) && sqlText[pos] == '/' && sqlText[pos+1] == '*' {
		i := pos + 2
		for i+1 < len(sqlText) && !(sqlText[i] == '*' && sqlText[i+1] == '/') {
			i++
		}
		if i+1 < len(sqlText) {
			i += 2
		} else {
			i = len(sqlText)
		}
		return i, true
	}
	return pos, false
}

func consumeEmptyCall(sqlText string, pos int) int {
	i := pos
	for i < len(sqlText) && isSQLSpace(sqlText[i]) {
		i++
	}
	if i >= len(sqlText) || sqlText[i] != '(' {
		return -1
	}
	i++
	for i < len(sqlText) && isSQLSpace(sqlText[i]) {
		i++
	}
	if i >= len(sqlText) || sqlText[i] != ')' {
		return -1
	}
	return i + 1
}

func isSQLIdentifierStart(ch byte) bool {
	return ch == '_' || ('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z')
}

func isSQLIdentifierPart(ch byte) bool {
	return isSQLIdentifierStart(ch) || ('0' <= ch && ch <= '9') || ch == '$'
}

func isSQLSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '\f':
		return true
	default:
		return false
	}
}
