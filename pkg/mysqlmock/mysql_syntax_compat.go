package mysqlmock

import (
	"strings"
)

func translateMySQLUpdateSetTargets(sqlText string) string {
	pos := skipSQLSpaces(sqlText, 0)
	if !consumeKeyword(sqlText, &pos, "UPDATE") {
		return sqlText
	}
	setStart := -1
	for pos < len(sqlText) {
		if end, ok := quotedSQLSpan(sqlText, pos); ok {
			pos = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, pos); ok {
			pos = end
			continue
		}
		if consumeKeyword(sqlText, &pos, "SET") {
			setStart = pos
			break
		}
		if _, end, ok := readSQLNameToken(sqlText, pos); ok {
			pos = end
			continue
		}
		pos++
	}
	if setStart < 0 {
		return sqlText
	}

	setEnd := len(sqlText)
	for i := setStart; i < len(sqlText); {
		if end, ok := quotedSQLSpan(sqlText, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			i = end
			continue
		}
		word, end, ok := readSQLIdentifier(sqlText, i)
		if ok && isMySQLUpdateSetTerminator(word) {
			setEnd = i
			break
		}
		if ok {
			i = end
			continue
		}
		i++
	}

	assignments := splitSQLTopLevelList(sqlText[setStart:setEnd])
	changed := false
	for i, assignment := range assignments {
		translated, ok := translateMySQLUpdateAssignmentTarget(assignment)
		if ok {
			assignments[i] = translated
			changed = true
		}
	}
	if !changed {
		return sqlText
	}
	return sqlText[:setStart] + " " + strings.Join(assignments, ", ") + sqlText[setEnd:]
}

func isMySQLUpdateSetTerminator(word string) bool {
	switch strings.ToUpper(word) {
	case "WHERE", "ORDER", "LIMIT":
		return true
	default:
		return false
	}
}

func translateMySQLUpdateAssignmentTarget(assignment string) (string, bool) {
	eq := topLevelEqualIndex(assignment)
	if eq < 0 {
		return assignment, false
	}
	left := strings.TrimSpace(assignment[:eq])
	target, ok := unqualifiedSQLAssignmentTarget(left)
	if !ok {
		return assignment, false
	}
	return target + " " + strings.TrimSpace(assignment[eq:]), true
}

func unqualifiedSQLAssignmentTarget(target string) (string, bool) {
	pos := 0
	parts := []string{}
	for {
		name, next, ok := readSQLNameToken(target, pos)
		if !ok {
			return "", false
		}
		parts = append(parts, name)
		pos = skipSQLSpaces(target, next)
		if pos >= len(target) {
			break
		}
		if target[pos] != '.' {
			return "", false
		}
		pos = skipSQLSpaces(target, pos+1)
	}
	if len(parts) < 2 {
		return "", false
	}
	return parts[len(parts)-1], true
}

func translateMySQLLikeDefaultEscape(sqlText string) string {
	if !canContainMySQLLikePredicate(sqlText) {
		return sqlText
	}

	var out strings.Builder
	out.Grow(len(sqlText))
	pending := false
	pendingDepth := 0
	hasEscape := false
	depth := 0

	insertEscape := func() {
		if pending && !hasEscape {
			out.WriteString(" ESCAPE char(92)")
		}
		pending = false
		hasEscape = false
	}

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
		if sqlText[i] == '(' {
			depth++
			out.WriteByte(sqlText[i])
			i++
			continue
		}
		if sqlText[i] == ')' {
			if pending && depth == pendingDepth {
				insertEscape()
			}
			if depth > 0 {
				depth--
			}
			out.WriteByte(sqlText[i])
			i++
			continue
		}
		if pending && depth == pendingDepth && (sqlText[i] == ',' || sqlText[i] == ';') {
			insertEscape()
			out.WriteByte(sqlText[i])
			i++
			continue
		}
		word, end, ok := readSQLIdentifier(sqlText, i)
		if !ok {
			out.WriteByte(sqlText[i])
			i++
			continue
		}
		upper := strings.ToUpper(word)
		if pending && depth == pendingDepth {
			if upper == "ESCAPE" {
				hasEscape = true
			} else if isMySQLLikePredicateTerminator(upper) {
				insertEscape()
			}
		}
		out.WriteString(sqlText[i:end])
		if upper == "LIKE" {
			pending = true
			pendingDepth = depth
			hasEscape = false
		}
		i = end
	}
	insertEscape()
	return out.String()
}

func canContainMySQLLikePredicate(sqlText string) bool {
	upper := strings.ToUpper(normalizeSQL(sqlText))
	return strings.HasPrefix(upper, "SELECT ") ||
		strings.HasPrefix(upper, "WITH ") ||
		strings.HasPrefix(upper, "UPDATE ") ||
		strings.HasPrefix(upper, "DELETE ") ||
		strings.HasPrefix(upper, "INSERT ")
}

func isMySQLLikePredicateTerminator(upper string) bool {
	switch upper {
	case "AND", "OR", "ORDER", "GROUP", "HAVING", "LIMIT", "UNION", "EXCEPT", "INTERSECT":
		return true
	default:
		return false
	}
}

func translateMySQLStringLiterals(sqlText string) string {
	var out strings.Builder
	out.Grow(len(sqlText))
	for i := 0; i < len(sqlText); {
		if sqlText[i] == '\'' {
			end, ok := quotedSQLSpan(sqlText, i)
			if !ok {
				out.WriteByte(sqlText[i])
				i++
				continue
			}
			out.WriteString(sqlStringLiteral(decodeMySQLStringLiteral(sqlText[i+1 : end-1])))
			i = end
			continue
		}
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
		out.WriteByte(sqlText[i])
		i++
	}
	return out.String()
}

func decodeMySQLStringLiteral(inner string) string {
	var out strings.Builder
	out.Grow(len(inner))
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\'' && i+1 < len(inner) && inner[i+1] == '\'' {
			out.WriteByte('\'')
			i++
			continue
		}
		if inner[i] != '\\' || i+1 >= len(inner) {
			out.WriteByte(inner[i])
			continue
		}
		i++
		switch inner[i] {
		case '0':
			out.WriteByte(0)
		case '\'':
			out.WriteByte('\'')
		case '"':
			out.WriteByte('"')
		case 'b':
			out.WriteByte('\b')
		case 'n':
			out.WriteByte('\n')
		case 'r':
			out.WriteByte('\r')
		case 't':
			out.WriteByte('\t')
		case 'Z':
			out.WriteByte(26)
		case '\\':
			out.WriteByte('\\')
		case '%', '_':
			out.WriteByte('\\')
			out.WriteByte(inner[i])
		default:
			out.WriteByte(inner[i])
		}
	}
	return out.String()
}

func hasMySQLBackslashEscapedString(sqlText string) bool {
	for i := 0; i < len(sqlText); {
		if sqlText[i] == '\'' {
			end, ok := quotedSQLSpan(sqlText, i)
			if !ok {
				return false
			}
			if strings.Contains(sqlText[i+1:end-1], `\`) {
				return true
			}
			i = end
			continue
		}
		if end, ok := quotedSQLSpan(sqlText, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			i = end
			continue
		}
		i++
	}
	return false
}

func hasSQLIdentifier(sqlText, want string) bool {
	for i := 0; i < len(sqlText); {
		if end, ok := quotedSQLSpan(sqlText, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(sqlText, i); ok {
			i = end
			continue
		}
		ident, end, ok := readSQLIdentifier(sqlText, i)
		if !ok {
			i++
			continue
		}
		if strings.EqualFold(ident, want) {
			return true
		}
		i = end
	}
	return false
}

func hasMySQLQualifiedUpdateSetTarget(sqlText string) bool {
	translated := translateMySQLUpdateSetTargets(sqlText)
	return translated != sqlText
}
