package mysqlmock

import (
	"strings"
	"testing"
)

var (
	benchmarkRuleConfig  RuleConfig
	benchmarkRuleMatched bool
)

func TestPreparedRulesMatchEquivalentRuleModes(t *testing.T) {
	rules := []RuleConfig{
		{
			Name:    "exact",
			Request: RuleRequestConfig{Match: "exact", SQL: "SELECT 1"},
		},
		{
			Name:    "normalized",
			Request: RuleRequestConfig{Match: "normalized", SQL: "select 2"},
		},
		{
			Name:    "regex",
			Request: RuleRequestConfig{Match: "regex", SQL: `^SELECT 3$`},
		},
		{
			Name:    "contains",
			Request: RuleRequestConfig{Match: "contains", SQL: "four"},
		},
		{
			Name:    "params",
			Request: RuleRequestConfig{Match: "exact", SQL: "SELECT ?", Params: []any{int64(5)}},
		},
		{
			Name:    "any",
			Request: RuleRequestConfig{Match: "any"},
		},
	}
	prepared, err := prepareRules(rules)
	if err != nil {
		t.Fatalf("prepare rules: %v", err)
	}
	server := &Server{rules: prepared}

	tests := []struct {
		name string
		sql  string
		args []any
		want string
	}{
		{name: "exact", sql: "SELECT 1", want: "exact"},
		{name: "normalized", sql: "  SELECT\n2 ;", want: "normalized"},
		{name: "regex", sql: "SELECT 3", want: "regex"},
		{name: "contains", sql: "SELECT 'four'", want: "contains"},
		{name: "params", sql: "SELECT ?", args: []any{[]byte("5")}, want: "params"},
		{name: "any", sql: "SELECT 6", want: "any"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule, ok, err := server.matchRule(tt.sql, tt.args)
			if err != nil {
				t.Fatalf("match rule: %v", err)
			}
			if !ok {
				t.Fatal("matched = false, want true")
			}
			if rule.Name != tt.want {
				t.Fatalf("matched rule = %q, want %q", rule.Name, tt.want)
			}
		})
	}
}

func TestPreparedRulesPreserveOnceBehavior(t *testing.T) {
	rules := []RuleConfig{
		{
			Name:    "once",
			Request: RuleRequestConfig{Match: "contains", SQL: "once_rule"},
			Response: RuleResponseConfig{
				Once: true,
			},
		},
		{
			Name:    "fallback",
			Request: RuleRequestConfig{Match: "any"},
		},
	}
	prepared, err := prepareRules(rules)
	if err != nil {
		t.Fatalf("prepare rules: %v", err)
	}
	server := &Server{rules: prepared}

	rule, ok, err := server.matchRule("SELECT 'once_rule'", nil)
	if err != nil {
		t.Fatalf("first match rule: %v", err)
	}
	if !ok || rule.Name != "once" {
		t.Fatalf("first matched rule = %q, %v, want once true", rule.Name, ok)
	}

	rule, ok, err = server.matchRule("SELECT 'once_rule'", nil)
	if err != nil {
		t.Fatalf("second match rule: %v", err)
	}
	if !ok || rule.Name != "fallback" {
		t.Fatalf("second matched rule = %q, %v, want fallback true", rule.Name, ok)
	}
}

func TestPrepareRulesRejectsInvalidRegex(t *testing.T) {
	_, err := prepareRules([]RuleConfig{{
		Name:    "bad regex",
		Request: RuleRequestConfig{Match: "regex", SQL: "["},
	}})
	if err == nil {
		t.Fatal("expected invalid regex error")
	}
	if !strings.Contains(err.Error(), "invalid request.sql regex") {
		t.Fatalf("error = %v, want invalid request.sql regex", err)
	}
}

func BenchmarkRuleMatching(b *testing.B) {
	for _, count := range []int{1, 10, 100} {
		for _, match := range []string{"exact", "normalized", "regex"} {
			b.Run(match+"/rules_"+stringForCacheTestInt(count)+"/hit_last", func(b *testing.B) {
				rules := benchmarkMatchRules(match, count)
				benchmarkRuleMatching(b, rules, benchmarkRuleQuery(match, count-1), true)
			})
			b.Run(match+"/rules_"+stringForCacheTestInt(count)+"/miss", func(b *testing.B) {
				rules := benchmarkMatchRules(match, count)
				benchmarkRuleMatching(b, rules, benchmarkRuleQuery(match, count), false)
			})
		}
		b.Run("any/rules_"+stringForCacheTestInt(count)+"/fallback", func(b *testing.B) {
			rules := benchmarkAnyFallbackRules(count)
			benchmarkRuleMatching(b, rules, "SELECT benchmark_any_fallback", true)
		})
	}
}

func benchmarkRuleMatching(b *testing.B, rules []RuleConfig, sqlText string, wantMatched bool) {
	b.Helper()

	prepared, err := prepareRules(rules)
	if err != nil {
		b.Fatalf("prepare rules: %v", err)
	}
	server := &Server{rules: prepared}

	rule, matched, err := server.matchRule(sqlText, nil)
	if err != nil {
		b.Fatalf("match rule: %v", err)
	}
	if matched != wantMatched {
		b.Fatalf("matched = %v, want %v", matched, wantMatched)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rule, matched, err = server.matchRule(sqlText, nil)
		if err != nil {
			b.Fatalf("match rule: %v", err)
		}
	}
	benchmarkRuleConfig = rule
	benchmarkRuleMatched = matched
}

func benchmarkMatchRules(match string, count int) []RuleConfig {
	rules := make([]RuleConfig, count)
	for i := range rules {
		rules[i] = RuleConfig{
			Name: "benchmark " + match + " " + stringForCacheTestInt(i),
			Request: RuleRequestConfig{
				Match: match,
				SQL:   benchmarkRulePattern(match, i),
			},
		}
	}
	return rules
}

func benchmarkAnyFallbackRules(count int) []RuleConfig {
	rules := make([]RuleConfig, count)
	for i := 0; i < count-1; i++ {
		rules[i] = RuleConfig{
			Name: "benchmark exact miss " + stringForCacheTestInt(i),
			Request: RuleRequestConfig{
				Match: "exact",
				SQL:   "SELECT benchmark_exact_miss_" + stringForCacheTestInt(i),
			},
		}
	}
	rules[count-1] = RuleConfig{
		Name:    "benchmark any fallback",
		Request: RuleRequestConfig{Match: "any"},
	}
	return rules
}

func benchmarkRulePattern(match string, index int) string {
	value := stringForCacheTestInt(index)
	switch match {
	case "exact":
		return "SELECT benchmark_exact_" + value
	case "normalized":
		return "select benchmark_normalized_" + value
	case "regex":
		return `^SELECT benchmark_regex_` + value + `$`
	default:
		return "SELECT benchmark_unknown_" + value
	}
}

func benchmarkRuleQuery(match string, index int) string {
	value := stringForCacheTestInt(index)
	switch match {
	case "exact":
		return "SELECT benchmark_exact_" + value
	case "normalized":
		return "  SELECT\nbenchmark_normalized_" + value + " ;"
	case "regex":
		return "SELECT benchmark_regex_" + value
	default:
		return "SELECT benchmark_unknown_" + value
	}
}
