package mysqlmock

import (
	"errors"
	"fmt"
	"os"

	"go.yaml.in/yaml/v4"
)

// Config describes a mysqlmock server instance.
type Config struct {
	Version int                         `yaml:"version"`
	Server  ServerConfig                `yaml:"server"`
	Backend DatabaseConfig              `yaml:"database"`
	Schema  []string                    `yaml:"schema"`
	Seed    map[string][]map[string]any `yaml:"seed"`
	Compat  CompatConfig                `yaml:"compat"`
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
	Variables map[string]string `yaml:"variables"`
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
			Variables: map[string]string{
				"autocommit":               "1",
				"character_set_client":     "utf8mb4",
				"character_set_connection": "utf8mb4",
				"character_set_results":    "utf8mb4",
				"collation_connection":     "utf8mb4_general_ci",
				"max_allowed_packet":       "67108864",
				"sql_mode":                 "",
				"transaction_isolation":    "READ-COMMITTED",
				"version":                  "8.0.36-mock",
				"version_comment":          "mysqlmock",
			},
		},
	}
}

// LoadConfigFile reads a YAML mysqlmock config file.
func LoadConfigFile(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()

	cfg := DefaultConfig()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
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
	if c.Seed == nil {
		c.Seed = map[string][]map[string]any{}
	}
	if c.Compat.Variables == nil {
		c.Compat.Variables = map[string]string{}
	}
	for k, v := range def.Compat.Variables {
		if _, ok := c.Compat.Variables[k]; !ok {
			c.Compat.Variables[k] = v
		}
	}
	c.Compat.Variables["version"] = c.Server.MySQLVersion
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
	switch c.Backend.Mode {
	case "memory":
	case "file":
		if c.Backend.Path == "" {
			return errors.New("database.path is required when database.mode is file")
		}
	default:
		return fmt.Errorf("unsupported database mode: %s", c.Backend.Mode)
	}
	return nil
}
