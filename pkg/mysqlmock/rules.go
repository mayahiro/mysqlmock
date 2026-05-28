package mysqlmock

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var errRuleDisconnect = errors.New("rule requested disconnect")

type ruleResponseProfile struct {
	Type     string
	Code     uint16
	SQLState string
	Message  string
}

var ruleResponseProfiles = map[string]ruleResponseProfile{
	"deadlock": {
		Type:     "error",
		Code:     1213,
		SQLState: "40001",
		Message:  "Deadlock found when trying to get lock; try restarting transaction",
	},
	"lock_wait_timeout": {
		Type:     "error",
		Code:     1205,
		SQLState: "HY000",
		Message:  "Lock wait timeout exceeded; try restarting transaction",
	},
	"duplicate_key": {
		Type:     "error",
		Code:     mysqlErrDupEntry,
		SQLState: "23000",
		Message:  "Duplicate entry for key 'mysqlmock'",
	},
	"foreign_key_violation": {
		Type:     "error",
		Code:     mysqlErrNoReferenced,
		SQLState: "23000",
		Message:  "Cannot add or update a child row: a foreign key constraint fails",
	},
	"disconnect": {
		Type: "disconnect",
	},
}

func (s *Server) executeRule(ctx context.Context, sqlText string, args []any) (any, bool, error) {
	rule, ok, err := s.matchRule(sqlText, args)
	if err != nil || !ok {
		return nil, ok, err
	}

	if err := waitRuleDelay(ctx, rule.Response.DelayMS); err != nil {
		return nil, true, err
	}

	switch rule.Response.Type {
	case "ok":
		return okResult{
			AffectedRows: rule.Response.AffectedRows,
			LastInsertID: rule.Response.LastInsertID,
			Warnings:     rule.Response.Warnings,
		}, true, nil
	case "result_set":
		rs, err := buildRuleResultSet(rule.Response)
		return rs, true, err
	case "error":
		return nil, true, errPacket(rule.Response.Code, rule.Response.SQLState, rule.Response.Message)
	case "disconnect":
		if err := waitRuleDelay(ctx, rule.Response.AfterMS); err != nil {
			return nil, true, err
		}
		return nil, true, errRuleDisconnect
	default:
		return nil, true, errPacket(mysqlErrUnknown, "HY000", "Unsupported rule response type: "+rule.Response.Type)
	}
}

func (s *Server) matchRule(sqlText string, args []any) (RuleConfig, bool, error) {
	for i, rule := range s.cfg.Rules {
		matched, err := ruleMatches(rule.Request, sqlText, args)
		if err != nil {
			return RuleConfig{}, false, err
		}
		if !matched {
			continue
		}
		if rule.Response.Once {
			s.mu.Lock()
			if s.ruleOnceUsed == nil {
				s.ruleOnceUsed = map[int]bool{}
			}
			if s.ruleOnceUsed[i] {
				s.mu.Unlock()
				continue
			}
			s.ruleOnceUsed[i] = true
			s.mu.Unlock()
		}
		return rule, true, nil
	}
	return RuleConfig{}, false, nil
}

func applyRuleResponseProfile(resp *RuleResponseConfig) {
	profile, ok := lookupRuleResponseProfile(resp.Profile)
	if !ok {
		return
	}
	if resp.Type == "" {
		resp.Type = profile.Type
	}
	if resp.Code == 0 {
		resp.Code = profile.Code
	}
	if resp.SQLState == "" {
		resp.SQLState = profile.SQLState
	}
	if resp.Message == "" {
		resp.Message = profile.Message
	}
}

func lookupRuleResponseProfile(name string) (ruleResponseProfile, bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return ruleResponseProfile{}, false
	}
	profile, ok := ruleResponseProfiles[name]
	return profile, ok
}

func ruleMatches(req RuleRequestConfig, sqlText string, args []any) (bool, error) {
	if !ruleParamsMatch(req.Params, args) {
		return false, nil
	}

	match := req.Match
	if match == "" {
		match = "exact"
	}
	switch match {
	case "exact":
		return sqlText == req.SQL, nil
	case "normalized":
		return strings.EqualFold(normalizeSQL(sqlText), normalizeSQL(req.SQL)), nil
	case "regex":
		return regexp.MatchString(req.SQL, sqlText)
	case "contains":
		return strings.Contains(sqlText, req.SQL), nil
	case "any":
		return true, nil
	default:
		return false, fmt.Errorf("unsupported rule match: %s", req.Match)
	}
}

func ruleParamsMatch(want, got []any) bool {
	if want == nil {
		return true
	}
	if len(want) != len(got) {
		return false
	}
	for i := range want {
		if !ruleValueEqual(want[i], got[i]) {
			return false
		}
	}
	return true
}

func ruleValueEqual(want, got any) bool {
	if want == nil || got == nil {
		return want == got
	}
	if wantBytes, ok := want.([]byte); ok {
		want = string(wantBytes)
	}
	if gotBytes, ok := got.([]byte); ok {
		got = string(gotBytes)
	}
	return fmt.Sprint(want) == fmt.Sprint(got)
}

func buildRuleResultSet(resp RuleResponseConfig) (resultSet, error) {
	columns := make([]resultColumn, len(resp.Columns))
	for i, col := range resp.Columns {
		fieldType, ok := ruleColumnFieldType(col.Type)
		if !ok {
			return resultSet{}, fmt.Errorf("unsupported result_set column type %q", col.Type)
		}
		columns[i] = resultColumn{Name: col.Name, Type: fieldType}
	}

	rows := make([][]any, 0, len(resp.Rows))
	for rowIndex, row := range resp.Rows {
		values, err := ruleRowValues(resp.RowFormat, resp.Columns, row)
		if err != nil {
			return resultSet{}, fmt.Errorf("rows[%d]: %w", rowIndex, err)
		}
		rows = append(rows, values)
	}
	return resultSet{Columns: columns, Rows: rows}, nil
}

func ruleRowValues(rowFormat string, columns []RuleColumnConfig, row any) ([]any, error) {
	if rowFormat != "" && rowFormat != "array" && rowFormat != "object" {
		return nil, fmt.Errorf("unsupported row_format: %s", rowFormat)
	}
	if rowFormat == "object" {
		return ruleObjectRowValues(columns, row)
	}

	if values, ok := row.([]any); ok {
		if len(values) != len(columns) {
			return nil, fmt.Errorf("value count mismatch: got %d, want %d", len(values), len(columns))
		}
		return values, nil
	}
	if isRuleObjectRow(row) {
		return ruleObjectRowValues(columns, row)
	}
	if len(columns) == 1 {
		return []any{row}, nil
	}
	return nil, fmt.Errorf("unsupported row value %T", row)
}

func ruleObjectRowValues(columns []RuleColumnConfig, row any) ([]any, error) {
	values := make([]any, len(columns))
	for i, col := range columns {
		value, ok := ruleObjectRowLookup(row, col.Name)
		if !ok {
			return nil, fmt.Errorf("missing column %q", col.Name)
		}
		values[i] = value
	}
	return values, nil
}

func isRuleObjectRow(row any) bool {
	switch row.(type) {
	case map[string]any, map[any]any:
		return true
	default:
		return false
	}
}

func ruleObjectRowLookup(row any, key string) (any, bool) {
	switch values := row.(type) {
	case map[string]any:
		value, ok := values[key]
		return value, ok
	case map[any]any:
		value, ok := values[key]
		return value, ok
	default:
		return nil, false
	}
}

func ruleColumnFieldType(typeName string) (byte, bool) {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "", "CHAR", "VARCHAR", "TEXT", "STRING":
		return fieldTypeVarString, true
	case "TINYINT", "BOOL", "BOOLEAN":
		return fieldTypeTiny, true
	case "SMALLINT":
		return fieldTypeShort, true
	case "INT", "INTEGER", "MEDIUMINT":
		return fieldTypeLong, true
	case "BIGINT":
		return fieldTypeLongLong, true
	case "FLOAT":
		return fieldTypeFloat, true
	case "DOUBLE", "REAL":
		return fieldTypeDouble, true
	case "DECIMAL", "NUMERIC":
		return fieldTypeDecimal, true
	case "DATE":
		return fieldTypeDate, true
	case "NEWDATE":
		return fieldTypeNewDate, true
	case "TIME":
		return fieldTypeTime, true
	case "DATETIME":
		return fieldTypeDateTime, true
	case "TIMESTAMP":
		return fieldTypeTimestamp, true
	case "TINYBLOB":
		return fieldTypeTinyBlob, true
	case "MEDIUMBLOB":
		return fieldTypeMedBlob, true
	case "LONGBLOB":
		return fieldTypeLongBlob, true
	case "BLOB", "BINARY", "VARBINARY":
		return fieldTypeBlob, true
	case "JSON":
		return fieldTypeJSON, true
	case "ENUM":
		return fieldTypeEnum, true
	case "SET":
		return fieldTypeSet, true
	case "GEOMETRY":
		return fieldTypeGeometry, true
	default:
		return 0, false
	}
}

func validateRuleConfig(rule RuleConfig) error {
	responseType := rule.Response.Type
	if profile, ok := lookupRuleResponseProfile(rule.Response.Profile); rule.Response.Profile != "" {
		if !ok {
			return fmt.Errorf("unsupported response.profile: %s", rule.Response.Profile)
		}
		if responseType != "" && responseType != profile.Type {
			return fmt.Errorf("response.profile %s requires response.type %s", rule.Response.Profile, profile.Type)
		}
		if responseType == "" {
			responseType = profile.Type
		}
	}

	match := rule.Request.Match
	if match == "" {
		match = "exact"
	}
	switch match {
	case "exact", "normalized", "regex", "contains":
		if rule.Request.SQL == "" {
			return fmt.Errorf("request.sql is required for %s match", match)
		}
	case "any":
	default:
		return fmt.Errorf("unsupported request.match: %s", match)
	}
	if match == "regex" {
		if _, err := regexp.Compile(rule.Request.SQL); err != nil {
			return fmt.Errorf("invalid request.sql regex: %w", err)
		}
	}
	if rule.Response.DelayMS < 0 {
		return errors.New("response.delay_ms must be non-negative")
	}
	if rule.Response.AfterMS < 0 {
		return errors.New("response.after_ms must be non-negative")
	}

	switch responseType {
	case "ok":
	case "result_set":
		if len(rule.Response.Columns) == 0 {
			return errors.New("response.columns is required for result_set")
		}
		for i, col := range rule.Response.Columns {
			if col.Name == "" {
				return fmt.Errorf("response.columns[%d].name is required", i)
			}
		}
		if _, err := buildRuleResultSet(rule.Response); err != nil {
			return fmt.Errorf("invalid result_set: %w", err)
		}
	case "error":
	case "disconnect":
	default:
		return fmt.Errorf("unsupported response.type: %s", responseType)
	}
	return nil
}

func waitRuleDelay(ctx context.Context, delayMS int) error {
	if delayMS == 0 {
		return nil
	}
	timer := time.NewTimer(time.Duration(delayMS) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
