# mysqlmock

`mysqlmock` is a lightweight MySQL-protocol test server for Go tests that touch
database-backed code. It accepts connections from MySQL client drivers, routes
common MySQL queries, and executes stateful CRUD operations against a SQLite
backend.

It is designed for fast tests that need a real MySQL client connection without
starting Docker or an external MySQL server. It is not a production database and
does not aim for full MySQL compatibility.

Use mysqlmock when you want faster database-backed tests than containerized MySQL,
but more realistic coverage than query expectation mocks. It lets service tests
keep using real repositories and MySQL client behavior without starting an
external database. Full database semantics such as migrations, locking,
isolation, optimizer behavior, and MySQL-specific edge cases should still remain
covered by real database tests.
This makes mysqlmock useful not only for repository tests, but also for
service-level tests that should exercise real repository implementations without
starting MySQL.

## Status and Scope

The current implementation targets:

- Go `database/sql`
- `github.com/go-sql-driver/mysql`
- `PingContext`
- `QueryContext` and `ExecContext`
- `PrepareContext` and prepared statement execution
- `BeginTx`, `Commit`, `Rollback`, and savepoints

Prepared statements support the common scalar parameter types used by Go
repository tests: `NULL`, signed and unsigned integers, booleans, strings,
bytes, floats, doubles, `time.Time` values encoded as strings, and common MySQL
binary protocol aliases such as `NEWDATE`, `ENUM`, `SET`, and blob variants.
`interpolateParams=true` is optional.

Compatibility scope for `v0.1.0`:

| Area | Status |
| --- | --- |
| Go version | Go 1.25 or newer |
| Go SQL client | `database/sql` with `github.com/go-sql-driver/mysql` |
| Core operations | Ping, query, exec, prepared statements, transactions, savepoints |
| Schema setup | Inline schema, SQL dump files, common MySQL/TiDB DDL translation |
| Seed data | Inline rows and YAML, JSON, or CSV seed files |
| ORM behavior | Common GORM setup variables and ActiveRecord-style schema introspection |
| MySQL/TiDB comparison | Optional compatibility scenarios against real MySQL or TiDB |
| Out of scope | Full MySQL SQL parsing, optimizer behavior, real row locks, TLS, compression, `multiStatements=true`, `LOAD DATA LOCAL INFILE` |

## Installation

Add the library to a Go module:

```sh
go get github.com/mayahiro/mysqlmock/pkg/mysqlmock
```

Install the CLI:

```sh
go install github.com/mayahiro/mysqlmock/cmd/mysqlmock@latest
```

mysqlmock requires Go 1.25 or newer. The exact module Go version is declared in
`go.mod`.

## Library Usage

```go
package users_test

import (
    "context"
    "database/sql"
    "testing"

    "github.com/mayahiro/mysqlmock/pkg/mysqlmock"

    _ "github.com/go-sql-driver/mysql"
)

func TestRepository(t *testing.T) {
    ctx := context.Background()
    server := mysqlmock.Start(t, mysqlmock.ConfigFile("testdb.yaml"))

    db, err := sql.Open("mysql", server.DSN())
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    if err := db.PingContext(ctx); err != nil {
        t.Fatal(err)
    }

    // Reset restores the configured schema and seed data and clears diagnostics.
    if err := server.Reset(ctx); err != nil {
        t.Fatal(err)
    }
}
```

`Server.DSN()` returns a `go-sql-driver/mysql` DSN like:

```text
user:password@tcp(127.0.0.1:<port>)/mysqlmock?interpolateParams=true&charset=utf8mb4&parseTime=true
```

See [examples/basic](examples/basic) for a complete repository-style test.

## Configuration

Config files are YAML. `version`, `server`, and `database` are required
top-level sections. Other top-level sections are optional.

```yaml
version: 1

server:
  mysql_version: "8.0.36-mock"
  auth:
    mode: allow_any

database:
  engine: sqlite
  mode: memory
  shared: true

schema_files:
  - db/schema.sql

seed_files:
  - testdata/users.yaml
  - testdata/posts.json
  - testdata/tags.csv

seed_file_configs:
  - path: testdata/legacy_users.csv
    table: users
    null_values: ["NULL", "\\N"]
    infer_types: true

schema:
  - |
    CREATE TABLE users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE
    );

seed:
  users:
    - id: 1
      name: "Alice"
      email: "alice@example.com"

compat:
  profile: gorm
  allow_zero_dates: false
  implicit_defaults: false
  write_validation: strict

fallback:
  type: sqlite
  unsupported:
    type: error
    code: 1105
    sql_state: "HY000"
    message: "Unsupported query"
```

Schema and query fallback apply a small MySQL-to-SQLite translation for common
Repository-test SQL. `database.mode: memory` uses an in-memory SQLite backend.
Use `schema_files` to load DDL from SQL dump files before inline `schema`
statements.
Use `seed_files` to load seed rows from YAML, JSON, or CSV files before inline
`seed` rows. Use `seed_file_configs` when a CSV file needs an explicit table
name, custom NULL markers, or basic type inference.
Set `database.shared: false` to initialize a separate in-memory database for
each MySQL client connection. Set `database.mode: file` and `database.path` to
persist the SQLite database across mysqlmock server restarts.

See [docs/configuration.md](docs/configuration.md) for the full config
reference.

## SQL Rules and Diagnostics

`rules` override matching SQL before mysqlmock uses built-in compatibility
handlers or the SQLite backend. Supported match modes are `exact`,
`normalized`, `regex`, `contains`, and `any`. Supported response types are
`ok`, `result_set`, `error`, and `disconnect`.

```yaml
rules:
  - name: force duplicate email
    request:
      match: contains
      sql: "INSERT INTO users"
    response:
      profile: duplicate_key
      once: true

  - name: fixed version
    request:
      match: exact
      sql: "SELECT VERSION()"
    response:
      type: result_set
      columns:
        - name: "VERSION()"
          type: VARCHAR
      rows:
        - ["8.0.36-mock"]
```

For common fault injection, `response.profile` can expand to MySQL-like errors
or disconnect behavior. Supported profiles are `deadlock`,
`lock_wait_timeout`, `duplicate_key`, `foreign_key_violation`, and
`disconnect`.

Unsupported query diagnostics include raw SQL, normalized SQL, connection ID,
current database, route stage, and a generated rule snippet. Query events can
also be written as stable JSON for golden-file tests.

See [docs/rules-and-diagnostics.md](docs/rules-and-diagnostics.md) for rule,
logging, unsupported-query, and snapshot details.
For repository tests, call `mysqlmock.AssertNoUnsupported(t, server)` after the
workflow and use `server.Reset(ctx)` between cases to restore schema, seed data,
auto-increment state, rules, and diagnostics.

## Compatibility Notes

Built-in compatibility handlers cover common MySQL client and ORM setup queries,
including `SET NAMES`, `SET autocommit`, `SELECT VERSION()`, `SELECT @@...`,
`SHOW VARIABLES`, `SHOW TABLES`, ActiveRecord-style schema introspection
queries, and a small `information_schema` subset.

Built-in scalar compatibility functions include `DATABASE()`, `SCHEMA()`,
`USER()`, `CURRENT_USER()`, `CONNECTION_ID()`, `LAST_INSERT_ID()`,
`ROW_COUNT()`, `CHAR_LENGTH()`, `CHARACTER_LENGTH()`, and `CURDATE()`.

`information_schema.schemata`, `tables`, `columns`, `key_column_usage`,
`statistics`, `table_constraints`, `referential_constraints`, and
`check_constraints` are available as a small metadata subset derived from the
SQLite schema.

ActiveRecord-style schema introspection supports `SHOW FULL FIELDS`,
`SHOW CREATE TABLE`, and `SHOW KEYS`. `SHOW CREATE TABLE` prefers the original
configured MySQL/TiDB DDL while the table is unchanged, then falls back to the
runtime SQLite definition after table-altering DDL. Advisory lock functions such
as `GET_LOCK` and `RELEASE_LOCK` emulate simple connection-owned lock conflicts.
`SHOW KEYS` includes prefix length, expression, and visibility metadata when
the index was created through mysqlmock's MySQL-compatible DDL path.

Write validation maps common repository-test failures to MySQL-like errors,
including duplicate keys, foreign keys, NOT NULL, CHECK constraints, data too
long for character columns, incorrect integer values, and incorrect datetime
values. Set `compat.allow_zero_dates: true` to accept zero date parts such as
`'0000-00-00'` and `'0001-00-00 00:00:00'` for legacy data.
Set `compat.implicit_defaults: true` to emulate non-strict MySQL implicit
defaults for `NOT NULL` columns without explicit defaults.
Set `compat.write_validation: basic` to skip value pre-validation on successful
writes while keeping SQLite constraint error mapping, or `off` to return raw
SQLite errors as generic MySQL errors.

Schema and query fallback translate `TRUE`, `FALSE`, `NOW()`,
`CURRENT_TIMESTAMP()`, `AUTO_INCREMENT`, TiDB `AUTO_RANDOM`, common MySQL and
TiDB DDL options, table-level `PRIMARY KEY` / `UNIQUE KEY` / `KEY` definitions,
simple MySQL index DDL, and common `ALTER TABLE` / `RENAME TABLE` variants into
SQLite-compatible SQL where possible. `DROP DATABASE` / `DROP SCHEMA` are
accepted as no-op teardown statements.
`CREATE TABLE ... PARTITION BY ...` partition clauses are stripped for SQLite
execution. Integer columns declared with `ZEROFILL` use the declared display
width for simple result-set values.
When an `AUTO_INCREMENT` column belongs to a composite primary key, mysqlmock
keeps the composite key and strips SQLite `AUTOINCREMENT`; SQLite only supports
automatic rowid assignment for a single `INTEGER PRIMARY KEY`. mysqlmock keeps
the original MySQL metadata and fills omitted, `NULL`, `0`, or `DEFAULT` values
with MySQL-like sequence values on compatible `INSERT ... VALUES` statements.
For single-column `AUTO_INCREMENT`, mysqlmock keeps consumed values from being
reused after `ROLLBACK` or `ROLLBACK TO SAVEPOINT`, matching InnoDB more
closely than SQLite's default rollback behavior.
MySQL-visible index names remain table-scoped; mysqlmock maps them to private
SQLite index names internally to avoid SQLite's schema-wide index namespace.
Common scalar functions and operators used by ORM queries include `IFNULL`,
`COALESCE`, `CONCAT`, `CAST`, `DATE_FORMAT`, `JSON_EXTRACT`, `JSON_UNQUOTE`,
`CHAR_LENGTH`, `CHARACTER_LENGTH`, `CURDATE`, `RAND`, `FIND_IN_SET`, `FIELD`,
and `REGEXP`.
SQLite fallback also handles MySQL backslash escapes in string literals,
MySQL's default backslash escape for `LIKE` patterns, and table-qualified
`UPDATE ... SET table.column = ...` targets emitted by some ORMs.

The SQLite fallback also handles common MySQL repository-test syntax such as
`INSERT ... ON DUPLICATE KEY UPDATE` with `VALUES(column)`, ActiveRecord-style
row aliases, and insert-side `DEFAULT` values, `INSERT IGNORE`, and
`REPLACE INTO`. It also strips
`FOR UPDATE` locking clauses, including `NOWAIT` and `SKIP LOCKED`. mysqlmock
does not emulate real MySQL row locks.

## CLI

From an installed CLI:

```sh
mysqlmock serve --config testdb.yaml --listen 127.0.0.1:0 --print-dsn
mysqlmock serve --config testdb.yaml --verbose --log-format json
mysqlmock serve --config testdb.yaml --fail-on-unsupported
mysqlmock check --config testdb.yaml
mysqlmock dump-unsupported-template
mysqlmock dump-config-schema
```

From a source checkout, replace `mysqlmock` with `go run ./cmd/mysqlmock`.

Use `serve --fail-on-unsupported` to exit with an error when the server observes
unsupported SQL. Use `--verbose --log-format=json` to emit route-aware query
logs as JSON Lines. Use `dump-config-schema` to print a JSON Schema for config
files.

## Documentation

- [Configuration Reference](docs/configuration.md)
- [Rules and Diagnostics](docs/rules-and-diagnostics.md)
- [v0.1.1 Release Notes](docs/releases/v0.1.1.md)
- [v0.1.0 Release Notes](docs/releases/v0.1.0.md)
- [Architecture](ARCHITECTURE.md)
- [Basic Example](examples/basic)
- [Japanese README](README_ja.md)

## Development

```sh
make fmt
make vet
make test
make build
```

Set `MYSQLMOCK_REAL_MYSQL_DSN` to run the optional compatibility scenario that
compares CRUD, transaction, duplicate-key, upsert, `INSERT IGNORE`, and
`REPLACE INTO` behavior against a real MySQL database:

```sh
MYSQLMOCK_REAL_MYSQL_DSN='user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true' \
  go test ./pkg/mysqlmock -run TestRealMySQLCompatibilityScenario
```

Set `MYSQLMOCK_REAL_TIDB_DSN` to run the same scenario against TiDB:

```sh
MYSQLMOCK_REAL_TIDB_DSN='user:password@tcp(127.0.0.1:4000)/testdb?parseTime=true' \
  go test ./pkg/mysqlmock -run TestRealTiDBCompatibilityScenario
```

Set `MYSQLMOCK_CLIENT_COMPAT_COMMANDS` to a JSON array of external client
commands to run opt-in compatibility checks for other languages. Each command is
run without a shell and receives `MYSQLMOCK_HOST`, `MYSQLMOCK_PORT`,
`MYSQLMOCK_USER`, `MYSQLMOCK_PASSWORD`, `MYSQLMOCK_DATABASE`, `MYSQLMOCK_ADDR`,
and `MYSQLMOCK_DSN` in its environment. This hook is the recommended place to
run a Rails or ActiveRecord smoke script when Ruby dependencies are available.
See [examples/active_record_smoke](examples/active_record_smoke).

## Known Limitations

- Prepared statement support does not aim to cover every MySQL binary protocol
  type yet.
- `SET NAMES` records connection character set variables but does not transcode
  query or result data.
- `REGEXP` compatibility is backed by Go regular expressions and does not
  exactly match every MySQL regular expression edge case.
- `RAND(seed)` is deterministic for equal seeds but does not reproduce MySQL's
  full per-statement random sequence behavior.
- TLS, compression, `multiStatements=true`, and `LOAD DATA LOCAL INFILE` are
  not supported.
- MySQL-specific SQL compatibility is intentionally small and should be expanded
  from real unsupported-query reports.

## License

MIT. See [LICENSE](LICENSE).
