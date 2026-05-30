package mysqlmock

import (
	"context"
	"strings"
)

func (c *mysqlConn) execMySQLDDLCompatibility(ctx context.Context, sqlText string) (okResult, bool, error) {
	if isDropDatabaseStatement(sqlText) {
		return okResult{}, true, nil
	}
	if isDropTableStatement(sqlText) {
		return okResult{}, true, nil
	}
	return okResult{}, false, nil
}

func isCreateDatabaseStatement(sqlText string) bool {
	statements := splitSQLStatements(sqlText)
	if len(statements) != 1 {
		return false
	}
	sqlText = statements[0]

	pos := 0
	if !consumeCreateDatabaseKeyword(sqlText, &pos, "CREATE") {
		return false
	}
	if !consumeCreateDatabaseKeyword(sqlText, &pos, "DATABASE") && !consumeCreateDatabaseKeyword(sqlText, &pos, "SCHEMA") {
		return false
	}
	if consumeCreateDatabaseKeyword(sqlText, &pos, "IF") &&
		(!consumeCreateDatabaseKeyword(sqlText, &pos, "NOT") || !consumeCreateDatabaseKeyword(sqlText, &pos, "EXISTS")) {
		return false
	}
	_, next, ok := readSQLNameToken(sqlText, skipSQLSpacesAndComments(sqlText, pos))
	if !ok {
		return false
	}
	return consumeCreateDatabaseOptions(sqlText, next)
}

func consumeCreateDatabaseOptions(sqlText string, pos int) bool {
	for {
		pos = skipSQLSpacesAndComments(sqlText, pos)
		if pos >= len(sqlText) {
			return true
		}
		consumeCreateDatabaseKeyword(sqlText, &pos, "DEFAULT")

		switch {
		case consumeCreateDatabaseKeyword(sqlText, &pos, "CHARACTER"):
			if !consumeCreateDatabaseKeyword(sqlText, &pos, "SET") {
				return false
			}
		case consumeCreateDatabaseKeyword(sqlText, &pos, "CHARSET"):
		case consumeCreateDatabaseKeyword(sqlText, &pos, "COLLATE"):
		case consumeCreateDatabaseKeyword(sqlText, &pos, "ENCRYPTION"):
		default:
			return false
		}

		pos = skipSQLSpacesAndComments(sqlText, pos)
		if pos < len(sqlText) && sqlText[pos] == '=' {
			pos++
		}
		next, ok := consumeCreateDatabaseOptionValue(sqlText, pos)
		if !ok {
			return false
		}
		pos = next
	}
}

func consumeCreateDatabaseOptionValue(sqlText string, pos int) (int, bool) {
	pos = skipSQLSpacesAndComments(sqlText, pos)
	if pos >= len(sqlText) {
		return pos, false
	}
	if end, ok := quotedSQLSpan(sqlText, pos); ok {
		return end, true
	}
	_, end, ok := readSQLIdentifier(sqlText, pos)
	return end, ok
}

func consumeCreateDatabaseKeyword(sqlText string, pos *int, keyword string) bool {
	word, next, ok := readSQLIdentifier(sqlText, skipSQLSpacesAndComments(sqlText, *pos))
	if !ok || !strings.EqualFold(word, keyword) {
		return false
	}
	*pos = next
	return true
}

func skipSQLSpacesAndComments(sqlText string, pos int) int {
	for {
		pos = skipSQLSpaces(sqlText, pos)
		if pos >= len(sqlText) {
			return pos
		}
		end, ok := sqlCommentSpan(sqlText, pos)
		if !ok {
			return pos
		}
		pos = end
	}
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

func isDropTableStatement(sqlText string) bool {
	statements := splitSQLStatements(sqlText)
	if len(statements) != 1 {
		return false
	}
	sqlText = statements[0]

	pos := 0
	if !consumeKeyword(sqlText, &pos, "DROP") {
		return false
	}
	_ = consumeKeyword(sqlText, &pos, "TEMPORARY")
	if !consumeKeyword(sqlText, &pos, "TABLE") {
		return false
	}
	if consumeKeyword(sqlText, &pos, "IF") {
		if !consumeKeyword(sqlText, &pos, "EXISTS") {
			return false
		}
	}

	for {
		_, next, ok := readSQLQualifiedName(sqlText, pos)
		if !ok {
			return false
		}
		pos = skipSQLSpaces(sqlText, next)
		if pos >= len(sqlText) || sqlText[pos] != ',' {
			break
		}
		pos++
	}

	switch {
	case consumeKeyword(sqlText, &pos, "CASCADE"):
	case consumeKeyword(sqlText, &pos, "RESTRICT"):
	}
	pos = skipSQLSpaces(sqlText, pos)
	return pos == len(sqlText)
}
