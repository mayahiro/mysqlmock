package mysqlmock

import (
	"reflect"
	"testing"
)

func TestSplitSQLTopLevelListSkipsNestedQuotedAndCommentCommas(t *testing.T) {
	input := "id, CONCAT(first_name, ', ', last_name), 'literal, value', /* comment, value */ created_at"
	want := []string{
		"id",
		" CONCAT(first_name, ', ', last_name)",
		" 'literal, value'",
		" /* comment, value */ created_at",
	}
	if got := splitSQLTopLevelList(input); !reflect.DeepEqual(got, want) {
		t.Fatalf("splitSQLTopLevelList() = %#v, want %#v", got, want)
	}
}

func TestFindTopLevelSQLKeywordSkipsNestedSelect(t *testing.T) {
	input := "SELECT id, (SELECT MAX(score) FROM scores) AS max_score FROM users"
	got, ok := findTopLevelSQLKeyword(input, len("SELECT "), "FROM")
	if !ok {
		t.Fatal("findTopLevelSQLKeyword() did not find top-level FROM")
	}
	want := len("SELECT id, (SELECT MAX(score) FROM scores) AS max_score ")
	if got != want {
		t.Fatalf("findTopLevelSQLKeyword() = %d, want %d", got, want)
	}
}

func TestParenthesizedSQLSpanSkipsQuotedParens(t *testing.T) {
	input := "(CONCAT(name, ')'), NOW()) trailing"
	got, ok := parenthesizedSQLSpan(input, 0)
	if !ok {
		t.Fatal("parenthesizedSQLSpan() did not find closing paren")
	}
	want := len("(CONCAT(name, ')'), NOW())")
	if got != want {
		t.Fatalf("parenthesizedSQLSpan() = %d, want %d", got, want)
	}
}

func TestReadSQLNameTokenReadsQuotedIdentifier(t *testing.T) {
	got, pos, ok := readSQLNameToken("  `user-table` next", 0)
	if !ok {
		t.Fatal("readSQLNameToken() did not read quoted identifier")
	}
	if got != "`user-table`" {
		t.Fatalf("readSQLNameToken() token = %q, want %q", got, "`user-table`")
	}
	if pos != len("  `user-table`") {
		t.Fatalf("readSQLNameToken() pos = %d, want %d", pos, len("  `user-table`"))
	}
}
