# Architecture

mysqlmock is a MySQL-protocol test double backed by SQLite. It exists to make Go
repository tests fast and deterministic while still exercising real MySQL client
drivers.

It is intentionally not a full MySQL-compatible database. Compatibility is added
around observed repository-test and ORM initialization behavior.

## Goals

- Accept MySQL client connections from `database/sql` drivers.
- Provide stateful CRUD behavior without running MySQL.
- Load schema and seed data from a small YAML config.
- Support transactions, prepared statements, and common ORM setup queries.
- Surface unsupported SQL with actionable diagnostics.
- Keep the implementation small enough to reason about in tests.

## Non-Goals

- Reimplement the MySQL optimizer, storage engine, replication, or binlog.
- Precisely emulate MySQL locking, gap locks, or isolation behavior.
- Parse and translate all MySQL SQL.
- Replace MySQL in production or performance benchmarks.

## Components

```text
Go tests / repositories / ORMs
        |
        | MySQL wire protocol
        v
mysqlmock server
        |
        +-- public API and CLI
        +-- config loading and validation
        +-- protocol layer
        +-- query router
        +-- SQLite backend
        +-- diagnostics
```

### Public API and CLI

`pkg/mysqlmock` is the public library package. The common test entrypoint is
`mysqlmock.Start(t, ...)`, which starts a server and registers `t.Cleanup`.

`cmd/mysqlmock` provides a CLI for long-running test servers, config checks,
unsupported-query rule templates, and config schema output.

### Config Loading

YAML config is decoded with known-field validation and then defaulted. Config
validation rejects unsupported public behavior such as unknown auth modes,
database engines, fallback types, rule match modes, rule response types, and
invalid SQL states.

The generated JSON Schema mirrors the supported config shape for editor and CI
validation.

### Protocol Layer

The protocol layer implements the subset of MySQL wire protocol needed by the
current compatibility target:

- handshake using `mysql_native_password` as the advertised plugin name
- permissive `allow_any` authentication
- `COM_QUERY`
- `COM_PING`
- `COM_QUIT`
- `COM_INIT_DB`
- prepared statement commands for prepare, execute, long data, close, and reset
- OK, ERR, text result set, and binary result set packets

The protocol layer tracks connection-local state such as current database,
character set variables, autocommit status, transaction status, last insert ID,
row count, and prepared statements.

### Query Router

Every query goes through the same routing order:

```text
1. config rules
2. built-in compatibility handlers
3. SQLite fallback after small SQL translation
4. unsupported query error
```

Rules are intentionally first so tests can override or inject failures before
mysqlmock handles a query internally.

Built-in compatibility handlers cover common client and ORM setup queries such
as `SET NAMES`, `SET autocommit`, version variables, `SHOW VARIABLES`, `SHOW
TABLES`, scalar compatibility functions, and a small `information_schema`
subset.

SQLite fallback handles normal CRUD after lightweight translation. Configured
schema setup uses the same focused translator for repository-test DDL, including
TiDB DDL, while single-statement runtime schema-changing DDL is accepted as a
no-op to preserve the configured schema during framework setup. Multi-statement
schema-changing queries remain unsupported. The translator is token-oriented
and deliberately small. It includes focused compatibility for common MySQL
upsert syntax including insert-side `DEFAULT` values and lock-clause stripping;
unsupported MySQL-specific SQL should be captured through diagnostics and
handled by a rule or another focused compatibility addition.

### SQLite Backend

SQLite provides the stateful execution engine.

`database.mode: memory` with `database.shared: true` gives all client
connections one shared in-memory database. `database.shared: false` gives each
client connection an isolated in-memory database initialized from schema and
seed data. `database.mode: file` stores state in a SQLite file.

Schema is applied before seed data. `Server.Reset` drops runtime SQLite objects,
reapplies schema and seed data, and clears query diagnostics and one-shot rule
state.

### Diagnostics

mysqlmock records unsupported queries and routed query events. The CLI can fail
on unsupported SQL and print generated rule snippets. The library can return
query snapshots as deterministic JSON for golden-file tests, omitting volatile
connection IDs.

## Compatibility Strategy

Compatibility grows from concrete test needs:

1. Prefer a config rule when a project needs one fixed query response or a
   targeted fault.
2. Add a built-in handler when many clients or ORMs issue the same setup query.
3. Extend SQL translation only for small, well-scoped MySQL syntax that maps
   predictably to SQLite.
4. Keep unsupported SQL visible so users can decide whether to add a rule,
   adjust their test schema, or request a new compatibility feature.

This keeps mysqlmock useful for repository tests without hiding the fact that it
is not a complete MySQL server.
