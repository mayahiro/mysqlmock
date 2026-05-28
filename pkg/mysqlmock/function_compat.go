package mysqlmock

import "strings"

func translateMySQLOperators(sqlText string) string {
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
		if strings.HasPrefix(sqlText[i:], "<=>") {
			out.WriteString(" IS ")
			i += len("<=>")
			continue
		}
		out.WriteByte(sqlText[i])
		i++
	}
	return out.String()
}

func translateMySQLFunctionCalls(sqlText string) string {
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
		ident, end, ok := readSQLIdentifier(sqlText, i)
		if !ok {
			out.WriteByte(sqlText[i])
			i++
			continue
		}
		callStart := skipSQLSpaces(sqlText, end)
		callEnd, hasCall := parenthesizedSQLSpan(sqlText, callStart)
		if !hasCall {
			out.WriteString(sqlText[i:end])
			i = end
			continue
		}
		if replacement, handled := translateMySQLFunctionCall(ident, sqlText[callStart+1:callEnd-1]); handled {
			out.WriteString(replacement)
			i = callEnd
			continue
		}
		out.WriteString(sqlText[i:end])
		i = end
	}
	return out.String()
}

func translateMySQLFunctionCall(name, inner string) (string, bool) {
	switch strings.ToUpper(name) {
	case "CONCAT":
		args := splitSQLTopLevelList(inner)
		if len(args) == 0 {
			return "''", true
		}
		for i, arg := range args {
			args[i] = "(" + translateMySQLFunctionCalls(strings.TrimSpace(arg)) + ")"
		}
		return strings.Join(args, " || "), true
	case "DATE_FORMAT":
		args := splitSQLTopLevelList(inner)
		if len(args) != 2 {
			return "", false
		}
		format := mysqlDateFormatToSQLite(unquoteSQLWord(strings.TrimSpace(args[1])))
		return "strftime(" + sqlStringLiteral(format) + ", " + translateMySQLFunctionCalls(strings.TrimSpace(args[0])) + ")", true
	case "JSON_EXTRACT":
		args := splitSQLTopLevelList(inner)
		if len(args) < 2 {
			return "", false
		}
		for i, arg := range args {
			args[i] = translateMySQLFunctionCalls(strings.TrimSpace(arg))
		}
		expr := "json_extract(" + strings.Join(args, ", ") + ")"
		if len(args) == 2 {
			return "json_quote(" + expr + ")", true
		}
		return expr, true
	case "JSON_UNQUOTE":
		args := splitSQLTopLevelList(inner)
		if len(args) != 1 {
			return "", false
		}
		return trimJSONQuoteExpression(strings.TrimSpace(args[0])), true
	case "CAST":
		expr, typ, ok := splitMySQLCast(inner)
		if !ok {
			return "", false
		}
		return "CAST(" + translateMySQLFunctionCalls(expr) + " AS " + sqliteCastType(typ) + ")", true
	default:
		return "", false
	}
}

func trimJSONQuoteExpression(expr string) string {
	translated := translateMySQLFunctionCalls(expr)
	trimmed := strings.TrimSpace(translated)
	const prefix = "json_quote("
	if strings.HasPrefix(trimmed, prefix) && strings.HasSuffix(trimmed, ")") {
		return strings.TrimSpace(trimmed[len(prefix) : len(trimmed)-1])
	}
	return "json_extract(" + trimmed + ", '$')"
}

func splitMySQLCast(inner string) (expr, typ string, ok bool) {
	for i := 0; i < len(inner); {
		if end, ok := quotedSQLSpan(inner, i); ok {
			i = end
			continue
		}
		if end, ok := sqlCommentSpan(inner, i); ok {
			i = end
			continue
		}
		word, end, ok := readSQLIdentifier(inner, i)
		if ok && strings.EqualFold(word, "AS") {
			expr = strings.TrimSpace(inner[:i])
			typ = strings.TrimSpace(inner[end:])
			return expr, typ, expr != "" && typ != ""
		}
		if ok {
			i = end
			continue
		}
		i++
	}
	return "", "", false
}

func sqliteCastType(mysqlType string) string {
	typ := strings.ToUpper(strings.TrimSpace(mysqlType))
	switch {
	case strings.HasPrefix(typ, "SIGNED"), strings.HasPrefix(typ, "UNSIGNED"), strings.HasPrefix(typ, "INTEGER"), strings.HasPrefix(typ, "INT"):
		return "INTEGER"
	case strings.HasPrefix(typ, "DECIMAL"), strings.HasPrefix(typ, "NUMERIC"), strings.HasPrefix(typ, "DOUBLE"), strings.HasPrefix(typ, "FLOAT"), strings.HasPrefix(typ, "REAL"):
		return "REAL"
	case strings.HasPrefix(typ, "BINARY"), strings.Contains(typ, "BLOB"):
		return "BLOB"
	default:
		return "TEXT"
	}
}

func mysqlDateFormatToSQLite(format string) string {
	replacements := map[string]string{
		"%D": "%d",
		"%f": "%f",
		"%h": "%I",
		"%i": "%M",
		"%k": "%H",
		"%l": "%I",
		"%S": "%S",
		"%s": "%S",
		"%p": "%p",
		"%r": "%I:%M:%S %p",
		"%T": "%H:%M:%S",
	}
	for old, replacement := range replacements {
		format = strings.ReplaceAll(format, old, replacement)
	}
	return format
}
