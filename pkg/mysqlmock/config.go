package mysqlmock

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"
)

// Config describes a mysqlmock server instance.
type Config struct {
	Version     int                         `yaml:"version"`
	Server      ServerConfig                `yaml:"server"`
	Backend     DatabaseConfig              `yaml:"database"`
	SchemaFiles []string                    `yaml:"schema_files"`
	Schema      []string                    `yaml:"schema"`
	SeedFiles   []string                    `yaml:"seed_files"`
	SeedConfigs []SeedFileConfig            `yaml:"seed_file_configs"`
	Seed        map[string][]map[string]any `yaml:"seed"`
	Compat      CompatConfig                `yaml:"compat"`
	Rules       []RuleConfig                `yaml:"rules"`
	Fallback    FallbackConfig              `yaml:"fallback"`

	schemaBaseDir string
}

// SeedFileConfig describes one external seed file with optional decoding settings.
type SeedFileConfig struct {
	Path       string   `yaml:"path"`
	Format     string   `yaml:"format"`
	Table      string   `yaml:"table"`
	NullValues []string `yaml:"null_values"`
	InferTypes bool     `yaml:"infer_types"`
}

// ServerConfig contains listener and MySQL compatibility settings.
type ServerConfig struct {
	Listen            string     `yaml:"listen"`
	MySQLVersion      string     `yaml:"mysql_version"`
	ConnectionIDStart uint32     `yaml:"connection_id_start"`
	Auth              AuthConfig `yaml:"auth"`
}

// AuthConfig controls connection authentication.
type AuthConfig struct {
	Mode string `yaml:"mode"`
}

// DatabaseConfig controls the SQLite backend.
type DatabaseConfig struct {
	Engine string `yaml:"engine"`
	Mode   string `yaml:"mode"`
	Shared bool   `yaml:"shared"`
	Path   string `yaml:"path"`
}

// CompatConfig contains built-in MySQL compatibility values.
type CompatConfig struct {
	Profile         string            `yaml:"profile"`
	AllowZeroDates  bool              `yaml:"allow_zero_dates"`
	WriteValidation string            `yaml:"write_validation"`
	Variables       map[string]string `yaml:"variables"`
}

// FallbackConfig controls behavior after rules and built-in compatibility handlers.
type FallbackConfig struct {
	Type        string            `yaml:"type"`
	Unsupported UnsupportedConfig `yaml:"unsupported"`
}

// UnsupportedConfig controls errors returned for unsupported SQL.
type UnsupportedConfig struct {
	Type     string `yaml:"type"`
	Code     uint16 `yaml:"code"`
	SQLState string `yaml:"sql_state"`
	Message  string `yaml:"message"`
}

// RuleConfig overrides matching SQL before built-in compatibility handlers or SQLite fallback.
type RuleConfig struct {
	Name     string             `yaml:"name"`
	Request  RuleRequestConfig  `yaml:"request"`
	Response RuleResponseConfig `yaml:"response"`
}

// RuleRequestConfig describes how an incoming SQL statement is matched.
type RuleRequestConfig struct {
	Match  string `yaml:"match"`
	SQL    string `yaml:"sql"`
	Params []any  `yaml:"params"`
}

// RuleResponseConfig describes the response returned by a matching rule.
type RuleResponseConfig struct {
	Profile      string             `yaml:"profile"`
	Type         string             `yaml:"type"`
	Columns      []RuleColumnConfig `yaml:"columns"`
	RowFormat    string             `yaml:"row_format"`
	Rows         []any              `yaml:"rows"`
	AffectedRows uint64             `yaml:"affected_rows"`
	LastInsertID uint64             `yaml:"last_insert_id"`
	Warnings     uint16             `yaml:"warnings"`
	Code         uint16             `yaml:"code"`
	SQLState     string             `yaml:"sql_state"`
	Message      string             `yaml:"message"`
	DelayMS      int                `yaml:"delay_ms"`
	AfterMS      int                `yaml:"after_ms"`
	Once         bool               `yaml:"once"`
}

// RuleColumnConfig describes one result-set column returned by a rule.
type RuleColumnConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// DefaultConfig returns a minimal in-memory mysqlmock configuration.
func DefaultConfig() Config {
	return Config{
		Version: 1,
		Server: ServerConfig{
			Listen:            "127.0.0.1:0",
			MySQLVersion:      "8.0.36-mock",
			ConnectionIDStart: 1,
			Auth: AuthConfig{
				Mode: "allow_any",
			},
		},
		Backend: DatabaseConfig{
			Engine: "sqlite",
			Mode:   "memory",
			Shared: true,
		},
		Seed: map[string][]map[string]any{},
		Compat: CompatConfig{
			Profile:         "default",
			WriteValidation: "strict",
			Variables: map[string]string{
				"autocommit":               "1",
				"character_set_client":     "utf8mb4",
				"character_set_connection": "utf8mb4",
				"character_set_database":   "utf8mb4",
				"character_set_results":    "utf8mb4",
				"collation_connection":     "utf8mb4_general_ci",
				"collation_database":       "utf8mb4_general_ci",
				"foreign_key_checks":       "1",
				"max_allowed_packet":       "67108864",
				"sql_mode":                 "",
				"transaction_isolation":    "READ-COMMITTED",
				"version":                  "8.0.36-mock",
				"version_comment":          "mysqlmock",
			},
		},
		Fallback: FallbackConfig{
			Type: "sqlite",
			Unsupported: UnsupportedConfig{
				Type:     "error",
				Code:     mysqlErrUnknown,
				SQLState: "HY000",
				Message:  "Unsupported query",
			},
		},
	}
}

// LoadConfigFile reads a YAML mysqlmock config file.
func LoadConfigFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var topLevel map[string]any
	if err := yaml.Unmarshal(data, &topLevel); err != nil {
		return Config{}, err
	}
	if err := validateRequiredConfigFields(topLevel); err != nil {
		return Config{}, err
	}

	cfg := DefaultConfig()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}
	cfg.applyDefaults()
	cfg.schemaBaseDir = filepath.Dir(path)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func decodeSeedFile(path string, data []byte) (map[string][]map[string]any, error) {
	return decodeSeedFileConfig(SeedFileConfig{Path: path}, data)
}

func decodeSeedFileConfig(cfg SeedFileConfig, data []byte) (map[string][]map[string]any, error) {
	path := cfg.Path
	switch seedFileFormat(path, cfg.Format) {
	case ".yaml", ".yml", "":
		return decodeYAMLSeedFile(data)
	case ".json":
		return decodeJSONSeedFile(data)
	case ".csv":
		return decodeCSVSeedFile(cfg, data)
	default:
		return nil, fmt.Errorf("unsupported seed file extension: %s", seedFileFormat(path, cfg.Format))
	}
}

func seedFileFormat(path, format string) string {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		return strings.ToLower(filepath.Ext(path))
	}
	return "." + strings.TrimPrefix(format, ".")
}

func decodeYAMLSeedFile(data []byte) (map[string][]map[string]any, error) {
	var wrapped struct {
		Seed map[string][]map[string]any `yaml:"seed" json:"seed"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&wrapped); err == nil && wrapped.Seed != nil {
		return wrapped.Seed, nil
	}

	var seed map[string][]map[string]any
	dec = yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&seed); err != nil {
		return nil, err
	}
	if seed == nil {
		seed = map[string][]map[string]any{}
	}
	return seed, nil
}

func decodeJSONSeedFile(data []byte) (map[string][]map[string]any, error) {
	var wrapped struct {
		Seed map[string][]map[string]any `yaml:"seed" json:"seed"`
	}
	if err := json.Unmarshal(data, &wrapped); err == nil && wrapped.Seed != nil {
		return wrapped.Seed, nil
	}

	var seed map[string][]map[string]any
	if err := json.Unmarshal(data, &seed); err != nil {
		return nil, err
	}
	if seed == nil {
		seed = map[string][]map[string]any{}
	}
	return seed, nil
}

func decodeCSVSeedFile(cfg SeedFileConfig, data []byte) (map[string][]map[string]any, error) {
	path := cfg.Path
	tableName := strings.TrimSpace(cfg.Table)
	if tableName == "" {
		tableName = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	if tableName == "" {
		return nil, errors.New("CSV seed file name must include a table name")
	}

	reader := csv.NewReader(bytes.NewReader(data))
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	if len(header) == 0 {
		return nil, errors.New("CSV seed file header is empty")
	}
	header[0] = strings.TrimPrefix(header[0], "\ufeff")
	for i, name := range header {
		header[i] = strings.TrimSpace(name)
		if header[i] == "" {
			return nil, fmt.Errorf("CSV seed file has empty column name at position %d", i+1)
		}
	}

	rows := []map[string]any{}
	nullValues := seedCSVNullValues(cfg.NullValues)
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, value := range record {
			if nullValues[value] {
				row[header[i]] = nil
				continue
			}
			row[header[i]] = decodeCSVSeedValue(value, cfg.InferTypes)
		}
		rows = append(rows, row)
	}
	return map[string][]map[string]any{tableName: rows}, nil
}

func seedCSVNullValues(values []string) map[string]bool {
	if len(values) == 0 {
		values = []string{`\N`}
	}
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func decodeCSVSeedValue(value string, inferTypes bool) any {
	if !inferTypes {
		return value
	}
	trimmed := strings.TrimSpace(value)
	switch strings.ToLower(trimmed) {
	case "true":
		return true
	case "false":
		return false
	}
	if isIntegerLiteral(trimmed) {
		if parsed, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return parsed
		}
	}
	if isFloatLiteral(trimmed) {
		if parsed, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return parsed
		}
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed
		}
	}
	return value
}

func isIntegerLiteral(value string) bool {
	if value == "" {
		return false
	}
	if value[0] == '-' || value[0] == '+' {
		value = value[1:]
	}
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func isFloatLiteral(value string) bool {
	if !strings.ContainsAny(value, ".eE") {
		return false
	}
	if value == "" {
		return false
	}
	if value[0] == '-' || value[0] == '+' {
		value = value[1:]
	}
	digitSeen := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if '0' <= ch && ch <= '9' {
			digitSeen = true
			continue
		}
		if ch == '.' || ch == 'e' || ch == 'E' || ch == '-' || ch == '+' {
			continue
		}
		return false
	}
	return digitSeen
}

func validateRequiredConfigFields(topLevel map[string]any) error {
	for _, field := range []string{"version", "server", "database"} {
		value, ok := topLevel[field]
		if !ok || value == nil {
			return fmt.Errorf("%s is required", field)
		}
	}
	return nil
}

func (c *Config) applyDefaults() {
	def := DefaultConfig()
	if c.Version == 0 {
		c.Version = def.Version
	}
	if c.Server.Listen == "" {
		c.Server.Listen = def.Server.Listen
	}
	if c.Server.MySQLVersion == "" {
		c.Server.MySQLVersion = def.Server.MySQLVersion
	}
	if c.Server.ConnectionIDStart == 0 {
		c.Server.ConnectionIDStart = def.Server.ConnectionIDStart
	}
	if c.Server.Auth.Mode == "" {
		c.Server.Auth.Mode = def.Server.Auth.Mode
	}
	if c.Backend.Engine == "" {
		c.Backend.Engine = def.Backend.Engine
	}
	if c.Backend.Mode == "" {
		c.Backend.Mode = def.Backend.Mode
	}
	if c.Fallback.Type == "" {
		c.Fallback.Type = def.Fallback.Type
	}
	if c.Fallback.Unsupported.Type == "" {
		c.Fallback.Unsupported.Type = def.Fallback.Unsupported.Type
	}
	if c.Fallback.Unsupported.Code == 0 {
		c.Fallback.Unsupported.Code = def.Fallback.Unsupported.Code
	}
	if c.Fallback.Unsupported.SQLState == "" {
		c.Fallback.Unsupported.SQLState = def.Fallback.Unsupported.SQLState
	}
	if c.Fallback.Unsupported.Message == "" {
		c.Fallback.Unsupported.Message = def.Fallback.Unsupported.Message
	}
	if c.Seed == nil {
		c.Seed = map[string][]map[string]any{}
	}
	if c.Compat.Profile == "" {
		c.Compat.Profile = def.Compat.Profile
	} else {
		c.Compat.Profile = normalizeCompatProfile(c.Compat.Profile)
	}
	if c.Compat.WriteValidation == "" {
		c.Compat.WriteValidation = def.Compat.WriteValidation
	} else {
		c.Compat.WriteValidation = normalizeWriteValidationMode(c.Compat.WriteValidation)
	}
	if c.Compat.Variables == nil {
		c.Compat.Variables = map[string]string{}
	}
	for k, v := range def.Compat.Variables {
		if _, ok := c.Compat.Variables[k]; !ok {
			c.Compat.Variables[k] = v
		}
	}
	applyCompatProfile(c.Compat.Profile, c.Compat.Variables)
	c.Compat.Variables["version"] = c.Server.MySQLVersion
	for i := range c.Rules {
		if c.Rules[i].Request.Match == "" {
			c.Rules[i].Request.Match = "exact"
		}
		applyRuleResponseProfile(&c.Rules[i].Response)
		if c.Rules[i].Response.Type == "error" {
			if c.Rules[i].Response.Code == 0 {
				c.Rules[i].Response.Code = mysqlErrUnknown
			}
			if c.Rules[i].Response.SQLState == "" {
				c.Rules[i].Response.SQLState = "HY000"
			}
			if c.Rules[i].Response.Message == "" {
				c.Rules[i].Response.Message = "Unsupported query"
			}
		}
	}
}

// Validate checks config values that affect public behavior.
func (c Config) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("unsupported config version: %d", c.Version)
	}
	if c.Server.Auth.Mode != "allow_any" {
		return fmt.Errorf("unsupported auth mode: %s", c.Server.Auth.Mode)
	}
	if c.Backend.Engine != "sqlite" {
		return fmt.Errorf("unsupported database engine: %s", c.Backend.Engine)
	}
	if _, ok := lookupCompatProfile(c.Compat.Profile); !ok {
		return fmt.Errorf("unsupported compat profile: %s", c.Compat.Profile)
	}
	if _, ok := lookupWriteValidationMode(c.Compat.WriteValidation); !ok {
		return fmt.Errorf("unsupported compat.write_validation: %s", c.Compat.WriteValidation)
	}
	switch c.Backend.Mode {
	case "memory":
	case "file":
		if c.Backend.Path == "" {
			return errors.New("database.path is required when database.mode is file")
		}
	default:
		return fmt.Errorf("unsupported database mode: %s", c.Backend.Mode)
	}
	fallbackType := c.Fallback.Type
	if fallbackType == "" {
		fallbackType = "sqlite"
	}
	if fallbackType != "sqlite" {
		return fmt.Errorf("unsupported fallback type: %s", c.Fallback.Type)
	}
	unsupportedType := c.Fallback.Unsupported.Type
	if unsupportedType == "" {
		unsupportedType = "error"
	}
	if unsupportedType != "error" {
		return fmt.Errorf("unsupported fallback.unsupported.type: %s", c.Fallback.Unsupported.Type)
	}
	unsupportedSQLState := c.Fallback.Unsupported.SQLState
	if unsupportedSQLState == "" {
		unsupportedSQLState = "HY000"
	}
	if unsupportedSQLState != fixedSQLState(unsupportedSQLState) {
		return fmt.Errorf("fallback.unsupported.sql_state must be 5 characters: %s", c.Fallback.Unsupported.SQLState)
	}
	for i, seedFile := range c.SeedConfigs {
		if strings.TrimSpace(seedFile.Path) == "" {
			return fmt.Errorf("seed_file_configs[%d].path is required", i)
		}
		format := seedFileFormat(seedFile.Path, seedFile.Format)
		switch format {
		case ".yaml", ".yml", ".json", ".csv", "":
		default:
			return fmt.Errorf("seed_file_configs[%d].format is unsupported: %s", i, seedFile.Format)
		}
	}
	for i, rule := range c.Rules {
		if err := validateRuleConfig(rule); err != nil {
			return fmt.Errorf("rules[%d]: %w", i, err)
		}
	}
	return nil
}

type compatProfile struct {
	Variables map[string]string
}

var compatProfiles = map[string]compatProfile{
	"default": {},
	"gorm": {
		Variables: map[string]string{
			"character_set_server":   "utf8mb4",
			"collation_server":       "utf8mb4_0900_ai_ci",
			"foreign_key_checks":     "1",
			"lower_case_table_names": "0",
			"sql_auto_is_null":       "0",
			"system_time_zone":       "UTC",
			"time_zone":              "SYSTEM",
			"transaction_read_only":  "0",
			"tx_isolation":           "READ-COMMITTED",
			"tx_read_only":           "0",
			"unique_checks":          "1",
		},
	},
}

func applyCompatProfile(name string, variables map[string]string) {
	profile, ok := lookupCompatProfile(name)
	if !ok {
		return
	}
	for k, v := range profile.Variables {
		if _, ok := variables[k]; !ok {
			variables[k] = v
		}
	}
}

func lookupCompatProfile(name string) (compatProfile, bool) {
	profile, ok := compatProfiles[normalizeCompatProfile(name)]
	return profile, ok
}

func normalizeCompatProfile(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "default"
	}
	return name
}

func lookupWriteValidationMode(name string) (string, bool) {
	name = normalizeWriteValidationMode(name)
	switch name {
	case "strict", "basic", "off":
		return name, true
	default:
		return "", false
	}
}

func normalizeWriteValidationMode(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "strict"
	}
	return name
}
