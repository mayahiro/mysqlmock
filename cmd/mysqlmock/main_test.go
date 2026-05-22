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
			SQL:           "CREATE USER unsupported_user",
			NormalizedSQL: "CREATE USER unsupported_user",
			ConnectionID:  7,
			Command:       "COM_QUERY",
			CurrentDB:     "mysqlmock",
			RouteStage:    "unsupported",
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
		"normalized: CREATE USER unsupported_user",
		"connection_id: 7",
		"command: COM_QUERY",
		"database: mysqlmock",
		"route_stage: unsupported",
		"Suggested rule:",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
}
