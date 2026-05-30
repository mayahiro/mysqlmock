package mysqlmock

import "testing"

func TestIsCreateDatabaseStatement(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "create database",
			sql:  "CREATE DATABASE `rspec_test`",
			want: true,
		},
		{
			name: "create database with options",
			sql:  "CREATE DATABASE IF NOT EXISTS rspec_test DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci",
			want: true,
		},
		{
			name: "create schema with encryption",
			sql:  "CREATE SCHEMA rspec_schema ENCRYPTION = 'N';",
			want: true,
		},
		{
			name: "versioned comment options",
			sql:  "CREATE DATABASE `ignored` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;",
			want: true,
		},
		{
			name: "missing database name",
			sql:  "CREATE DATABASE",
			want: false,
		},
		{
			name: "invalid if clause",
			sql:  "CREATE DATABASE IF EXISTS rspec_test",
			want: false,
		},
		{
			name: "unexpected option",
			sql:  "CREATE DATABASE rspec_test UNKNOWN option",
			want: false,
		},
		{
			name: "multiple statements",
			sql:  "CREATE DATABASE rspec_test; DROP TABLE users",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCreateDatabaseStatement(tt.sql); got != tt.want {
				t.Fatalf("isCreateDatabaseStatement() = %v, want %v", got, tt.want)
			}
		})
	}
}
