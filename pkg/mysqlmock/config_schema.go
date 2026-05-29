package mysqlmock

const configSchemaJSON = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/mayahiro/mysqlmock/config.schema.json",
  "title": "mysqlmock config",
  "type": "object",
  "additionalProperties": false,
  "required": ["version", "server", "database"],
  "properties": {
    "version": {
      "type": "integer",
      "const": 1
    },
    "server": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "listen": {
          "type": "string"
        },
        "mysql_version": {
          "type": "string"
        },
        "connection_id_start": {
          "type": "integer",
          "minimum": 1
        },
        "auth": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "mode": {
              "type": "string",
              "enum": ["allow_any"]
            }
          }
        }
      }
    },
    "database": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "engine": {
          "type": "string",
          "enum": ["sqlite"]
        },
        "mode": {
          "type": "string",
          "enum": ["memory", "file"]
        },
        "shared": {
          "type": "boolean"
        },
        "path": {
          "type": "string"
        }
      },
      "allOf": [
        {
          "if": {
            "properties": {
              "mode": {
                "const": "file"
              }
            },
            "required": ["mode"]
          },
          "then": {
            "required": ["path"]
          }
        }
      ]
    },
    "schema": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "schema_files": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "seed": {
      "type": "object",
      "additionalProperties": {
        "type": "array",
        "items": {
          "type": "object",
          "additionalProperties": true
        }
      }
    },
    "seed_files": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "seed_file_configs": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["path"],
        "properties": {
          "path": {
            "type": "string"
          },
          "format": {
            "type": "string",
            "enum": ["yaml", "yml", "json", "csv"]
          },
          "table": {
            "type": "string"
          },
          "null_values": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "infer_types": {
            "type": "boolean"
          }
        }
      }
    },
    "compat": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "profile": {
          "type": "string",
          "enum": ["default", "gorm"]
        },
        "allow_zero_dates": {
          "type": "boolean"
        },
        "variables": {
          "type": "object",
          "additionalProperties": {
            "type": "string"
          }
        }
      }
    },
    "rules": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["request", "response"],
        "properties": {
          "name": {
            "type": "string"
          },
          "request": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "match": {
                "type": "string",
                "enum": ["exact", "normalized", "regex", "contains", "any"]
              },
              "sql": {
                "type": "string"
              },
              "params": {
                "type": "array"
              }
            },
            "allOf": [
              {
                "if": {
                  "not": {
                    "properties": {
                      "match": {
                        "const": "any"
                      }
                    },
                    "required": ["match"]
                  }
                },
                "then": {
                  "required": ["sql"]
                }
              }
            ]
          },
          "response": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "profile": {
                "type": "string",
                "enum": ["deadlock", "lock_wait_timeout", "duplicate_key", "foreign_key_violation", "disconnect"]
              },
              "type": {
                "type": "string",
                "enum": ["ok", "result_set", "error", "disconnect"]
              },
              "columns": {
                "type": "array",
                "items": {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["name"],
                  "properties": {
                    "name": {
                      "type": "string"
                    },
                    "type": {
                      "type": "string"
                    }
                  }
                }
              },
              "row_format": {
                "type": "string",
                "enum": ["array", "object"]
              },
              "rows": {
                "type": "array"
              },
              "affected_rows": {
                "type": "integer",
                "minimum": 0
              },
              "last_insert_id": {
                "type": "integer",
                "minimum": 0
              },
              "warnings": {
                "type": "integer",
                "minimum": 0,
                "maximum": 65535
              },
              "code": {
                "type": "integer",
                "minimum": 0,
                "maximum": 65535
              },
              "sql_state": {
                "type": "string",
                "minLength": 5,
                "maxLength": 5
              },
              "message": {
                "type": "string"
              },
              "delay_ms": {
                "type": "integer",
                "minimum": 0
              },
              "after_ms": {
                "type": "integer",
                "minimum": 0
              },
              "once": {
                "type": "boolean"
              }
            },
            "anyOf": [
              {
                "required": ["type"]
              },
              {
                "required": ["profile"]
              }
            ],
            "allOf": [
              {
                "if": {
                  "properties": {
                    "type": {
                      "const": "result_set"
                    }
                  },
                  "required": ["type"]
                },
                "then": {
                  "required": ["columns"]
                }
              }
            ]
          }
        }
      }
    },
    "fallback": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "type": {
          "type": "string",
          "enum": ["sqlite"]
        },
        "unsupported": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "type": {
              "type": "string",
              "enum": ["error"]
            },
            "code": {
              "type": "integer",
              "minimum": 0,
              "maximum": 65535
            },
            "sql_state": {
              "type": "string",
              "minLength": 5,
              "maxLength": 5
            },
            "message": {
              "type": "string"
            }
          }
        }
      }
    }
  }
}`

// ConfigSchemaJSON returns a JSON Schema for mysqlmock YAML/JSON config files.
func ConfigSchemaJSON() []byte {
	return []byte(configSchemaJSON)
}
