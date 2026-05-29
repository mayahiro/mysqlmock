# Configuration Reference

mysqlmock config files are YAML. The library also exposes the same shape through
`mysqlmock.Config` when using `mysqlmock.WithConfig`.

Unknown YAML fields are rejected when loading a file with `ConfigFile` or
`LoadConfigFile`. `version`, `server`, and `database` must be present at the
top level.

## Minimal Config

```yaml
version: 1
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
```

Missing nested values are filled from defaults.

## Top-Level Sections

| Field | Required | Description |
| --- | --- | --- |
| `version` | Yes | Config version. Only `1` is supported. |
| `server` | Yes | Listener and MySQL compatibility settings. |
| `database` | Yes | SQLite backend settings. |
| `schema` | No | SQL statements applied before seed data. |
| `schema_files` | No | SQL dump files applied before inline `schema`. |
| `seed` | No | Table rows inserted after schema setup. |
| `seed_files` | No | YAML, JSON, or CSV seed files inserted before inline `seed`. |
| `seed_file_configs` | No | External seed files with per-file CSV options. |
| `compat` | No | Built-in MySQL compatibility variables and profiles. |
| `rules` | No | SQL override and fault-injection rules. |
| `fallback` | No | Behavior after rules and built-in compatibility handlers. |

## Defaults

| Field | Default |
| --- | --- |
| `version` | `1` |
| `server.listen` | `127.0.0.1:0` |
| `server.mysql_version` | `8.0.36-mock` |
| `server.connection_id_start` | `1` |
| `server.auth.mode` | `allow_any` |
| `database.engine` | `sqlite` |
| `database.mode` | `memory` |
| `database.shared` | `true` |
| `compat.profile` | `default` |
| `fallback.type` | `sqlite` |
| `fallback.unsupported.type` | `error` |
| `fallback.unsupported.code` | `1105` |
| `fallback.unsupported.sql_state` | `HY000` |
| `fallback.unsupported.message` | `Unsupported query` |

## Server

```yaml
server:
  listen: "127.0.0.1:0"
  mysql_version: "8.0.36-mock"
  connection_id_start: 1
  auth:
    mode: allow_any
```

- `listen` is the TCP listen address. Port `0` asks the OS for a free port.
- `mysql_version` is advertised in the handshake and returned by version
  compatibility handlers.
- `connection_id_start` controls the first MySQL connection ID.
- `auth.mode` currently supports only `allow_any`. It accepts any user and
  password sent by the client.

## Database

```yaml
database:
  engine: sqlite
  mode: memory
  shared: true
```

`engine` currently supports only `sqlite`.

`mode: memory` creates an in-memory SQLite backend. With `shared: true`, client
connections share the same in-memory database. With `shared: false`, each MySQL
client connection gets a separate in-memory database initialized from the
configured `schema` and `seed`.

`mode: file` persists the SQLite database on disk and requires `path`:

```yaml
database:
  engine: sqlite
  mode: file
  path: "testdata/mysqlmock.sqlite"
```

Use file mode only when a test intentionally needs state to survive mysqlmock
server restarts.

## Schema

`schema` is an ordered list of SQL statements:

```yaml
schema:
  - |
    CREATE TABLE users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  - |
    CREATE INDEX idx_users_email ON users (email);
```

`schema_files` is an ordered list of SQL dump files. Paths in config files are
resolved relative to the config file directory. Files are applied before inline
`schema` statements:

```yaml
schema_files:
  - db/schema.sql
```

mysqlmock splits dump files into SQL statements and applies schema DDL such as
`DROP TABLE`, `CREATE TABLE`, `CREATE INDEX`, and `ALTER TABLE`. Dump helper
statements such as `SET`, `USE`, `LOCK TABLES`, `UNLOCK TABLES`, and data
`INSERT` statements are ignored. Use `mysqldump --no-data` or a TiDB Dumpling
`*-schema.sql` file when possible.

Each statement is translated with mysqlmock's small MySQL-to-SQLite translator
before being applied. The translator handles common Repository-test DDL such as
`AUTO_INCREMENT`, TiDB `AUTO_RANDOM`, TiDB clustered-index comments, `TRUE`,
`FALSE`, `NOW()`, `CURRENT_TIMESTAMP(n)`, common table options,
`AUTO_RANDOM_BASE`, table-level `PRIMARY KEY` / `UNIQUE KEY` / `KEY`
definitions, simple MySQL index DDL, and common `ALTER TABLE` / `RENAME TABLE`
variants. It does not try to implement a full MySQL parser.
If an `AUTO_INCREMENT` column is part of a composite primary key, mysqlmock
keeps the composite key and removes `AUTO_INCREMENT`; SQLite can only
auto-assign rowid values for a single `INTEGER PRIMARY KEY`, so that column must
be set explicitly by seed data or test inserts.
MySQL-visible index names are treated as table-scoped, and mysqlmock maps them
to private SQLite index names internally so the same index name can be used on
multiple tables in a schema dump.

## Seed Data

`seed` maps table names to rows:

```yaml
seed:
  users:
    - id: 1
      name: "Alice"
      email: "alice@example.com"
    - id: 2
      name: "Bob"
      email: "bob@example.com"
```

`seed_files` is an ordered list of external seed files. Paths in config files
are resolved relative to the config file directory. Files are inserted before
inline `seed` rows:

```yaml
seed_files:
  - testdata/users.yaml
  - testdata/posts.json
  - testdata/tags.csv
```

YAML and JSON seed files may either contain the same table map as `seed` or wrap
it under a `seed` key. CSV seed files use the file name without `.csv` as the
table name, the first row as column names, and `\N` as `NULL`.

Use `seed_file_configs` when a seed file needs per-file settings:

```yaml
seed_file_configs:
  - path: testdata/legacy_users.csv
    format: csv
    table: users
    null_values: ["NULL", "\\N"]
    infer_types: true
```

`table` overrides the CSV file-name table convention. `null_values` defaults to
`["\\N"]`. `infer_types: true` converts simple booleans, integers, floats, and
common datetime strings before inserting rows.

Seed data is inserted after all schema statements. Table and column names are
quoted when mysqlmock builds seed insert statements.

## Compatibility Variables

```yaml
compat:
  profile: gorm
  variables:
    lower_case_table_names: "1"
    time_zone: "SYSTEM"
```

The `default` profile includes common variables such as:

- `autocommit`
- `character_set_client`
- `character_set_connection`
- `character_set_database`
- `character_set_results`
- `collation_connection`
- `collation_database`
- `foreign_key_checks`
- `max_allowed_packet`
- `sql_mode`
- `transaction_isolation`
- `version`
- `version_comment`

The `gorm` profile adds common ORM initialization variables, including:

- `character_set_server`
- `collation_server`
- `lower_case_table_names`
- `sql_auto_is_null`
- `system_time_zone`
- `time_zone`
- `transaction_read_only`
- `tx_isolation`
- `tx_read_only`
- `unique_checks`

Explicit `compat.variables` values override profile defaults. `server.mysql_version`
always controls the effective `version` variable.

## Fallback

```yaml
fallback:
  type: sqlite
  unsupported:
    type: error
    code: 1105
    sql_state: "HY000"
    message: "Unsupported query"
```

`fallback.type` currently supports only `sqlite`. `fallback.unsupported` controls
the MySQL error returned when a query is not handled by rules, built-in
compatibility handlers, or SQLite fallback.

SQLite fallback includes focused repository-test compatibility for
`INSERT ... ON DUPLICATE KEY UPDATE` with `VALUES(column)`, ActiveRecord-style
row aliases, and insert-side `DEFAULT` values, `INSERT IGNORE`, and
`REPLACE INTO`. It also registers MySQL-compatible scalar functions for common
ORM and repository queries, including `RAND`, `FIND_IN_SET`, `FIELD`, and the
`REGEXP` operator. `REGEXP` uses Go regular expressions, so exact MySQL regular
expression dialect compatibility is not guaranteed. `RAND(seed)` is
deterministic for equal seeds but does not reproduce MySQL's full per-statement
random sequence behavior.

For MySQL-compatible DDL that creates indexes, mysqlmock also keeps lightweight
index metadata so `SHOW KEYS` can expose prefix length, expression, and
visibility fields used by ORM schema introspection.

When `SHOW CREATE TABLE` is requested, mysqlmock returns the original configured
MySQL/TiDB `CREATE TABLE` statement while that table has not been changed at
runtime. After table-altering DDL, the cached original DDL is invalidated and
the response falls back to the current SQLite definition with MySQL-style table
options.

## JSON Schema

Print the config JSON Schema with:

```sh
mysqlmock dump-config-schema
```

From a source checkout:

```sh
go run ./cmd/mysqlmock dump-config-schema
```

Validate that a config loads and that schema and seed data can be applied:

```sh
mysqlmock check --config testdb.yaml
```
