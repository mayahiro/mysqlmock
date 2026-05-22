package main

import (
	"strings"
	"testing"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"
)

func TestUnsupportedQueriesError(t *testing.T) {
	if err := unsupportedQueriesError(nil); err != nil {
		t.Fatalf("unsupportedQueriesError(nil) = %v", err)
	}

	err := unsupportedQueriesError([]mysqlmock.UnsupportedQuery{
		{
			SQL: "CREATE USER unsupported_user",
			Suggestion: "Suggested rule:\n" +
				"  - name: generated unsupported query",
		},
	})
	if err == nil {
		t.Fatal("expected unsupported queries error")
	}
	for _, want := range []string{
		"unsupported queries observed: 1",
		"CREATE USER unsupported_user",
		"Suggested rule:",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
}
