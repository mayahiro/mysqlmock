package mysqlmock_test

import (
	"encoding/json"
	"testing"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"
)

func TestConfigSchemaJSON(t *testing.T) {
	var schema map[string]any
	if err := json.Unmarshal(mysqlmock.ConfigSchemaJSON(), &schema); err != nil {
		t.Fatalf("config schema is not valid JSON: %v", err)
	}

	required, ok := schema["required"].([]any)
	if !ok {
		t.Fatalf("schema required = %#v, want array", schema["required"])
	}
	for _, want := range []string{"version", "server", "database"} {
		if !containsStringValue(required, want) {
			t.Fatalf("schema required = %#v, want %q", required, want)
		}
	}

	properties := schema["properties"].(map[string]any)
	if properties["rules"] == nil {
		t.Fatal("schema did not include rules")
	}
	if properties["fallback"] == nil {
		t.Fatal("schema did not include fallback")
	}
	if properties["schema_files"] == nil {
		t.Fatal("schema did not include schema_files")
	}
	if properties["seed_files"] == nil {
		t.Fatal("schema did not include seed_files")
	}

	compat := properties["compat"].(map[string]any)
	compatProperties := compat["properties"].(map[string]any)
	profile := compatProperties["profile"].(map[string]any)
	profileEnum := profile["enum"].([]any)
	for _, want := range []string{"default", "gorm"} {
		if !containsStringValue(profileEnum, want) {
			t.Fatalf("compat.profile enum = %#v, want %q", profileEnum, want)
		}
	}
}

func containsStringValue(values []any, want string) bool {
	for _, value := range values {
		if got, ok := value.(string); ok && got == want {
			return true
		}
	}
	return false
}
