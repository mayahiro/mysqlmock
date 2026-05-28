package mysqlmock

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRuleResponseProfiles(t *testing.T) {
	tests := []struct {
		profile  string
		wantCode uint16
		wantSQL  string
		wantText string
	}{
		{
			profile:  "deadlock",
			wantCode: 1213,
			wantSQL:  "40001",
			wantText: "Deadlock found",
		},
		{
			profile:  "lock_wait_timeout",
			wantCode: 1205,
			wantSQL:  "HY000",
			wantText: "Lock wait timeout exceeded",
		},
		{
			profile:  "duplicate_key",
			wantCode: mysqlErrDupEntry,
			wantSQL:  "23000",
			wantText: "Duplicate entry",
		},
		{
			profile:  "foreign_key_violation",
			wantCode: mysqlErrNoReferenced,
			wantSQL:  "23000",
			wantText: "foreign key constraint fails",
		},
	}

	for _, tt := range tests {
		t.Run(tt.profile, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Rules = []RuleConfig{{
				Request:  RuleRequestConfig{Match: "any"},
				Response: RuleResponseConfig{Profile: tt.profile},
			}}
			server, err := New(WithConfig(cfg))
			if err != nil {
				t.Fatalf("New(): %v", err)
			}

			_, matched, err := server.executeRule(context.Background(), "SELECT 1", nil)
			if !matched {
				t.Fatal("profile rule did not match")
			}
			var mysqlErr *mysqlError
			if !errors.As(err, &mysqlErr) {
				t.Fatalf("executeRule() error = %v, want mysqlError", err)
			}
			if mysqlErr.Code != tt.wantCode || mysqlErr.SQLState != tt.wantSQL || !strings.Contains(mysqlErr.Message, tt.wantText) {
				t.Fatalf("mysql error = code:%d sql:%s message:%q", mysqlErr.Code, mysqlErr.SQLState, mysqlErr.Message)
			}
		})
	}
}

func TestRuleResponseDisconnectProfile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rules = []RuleConfig{{
		Request:  RuleRequestConfig{Match: "any"},
		Response: RuleResponseConfig{Profile: "disconnect"},
	}}
	server, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatalf("New(): %v", err)
	}

	_, matched, err := server.executeRule(context.Background(), "SELECT 1", nil)
	if !matched {
		t.Fatal("profile rule did not match")
	}
	if !errors.Is(err, errRuleDisconnect) {
		t.Fatalf("executeRule() error = %v, want errRuleDisconnect", err)
	}
}

func TestRuleResponseProfileValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rules = []RuleConfig{{
		Request:  RuleRequestConfig{Match: "any"},
		Response: RuleResponseConfig{Profile: "unknown"},
	}}
	if _, err := New(WithConfig(cfg)); err == nil {
		t.Fatal("expected unsupported response profile error")
	}

	cfg = DefaultConfig()
	cfg.Rules = []RuleConfig{{
		Request:  RuleRequestConfig{Match: "any"},
		Response: RuleResponseConfig{Profile: "deadlock", Type: "ok"},
	}}
	if _, err := New(WithConfig(cfg)); err == nil {
		t.Fatal("expected mismatched response profile type error")
	}
}
