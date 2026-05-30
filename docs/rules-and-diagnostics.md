# Rules and Diagnostics

mysqlmock routes each SQL statement through this order:

```text
1. rules
2. built-in compatibility handlers
3. SQLite fallback with small MySQL-to-SQLite translation
4. unsupported query error
```

Rules let tests override exact SQL, return fixed result sets, inject MySQL-like
errors, add delays, or disconnect a client connection.

## Rule Shape

```yaml
rules:
  - name: fixed user lookup
    request:
      match: normalized
      sql: "SELECT id, name FROM users WHERE email = ?"
      params: ["alice@example.com"]
    response:
      type: result_set
      columns:
        - name: id
          type: BIGINT
        - name: name
          type: VARCHAR
      row_format: object
      rows:
        - id: 1
          name: "Alice"
```

`name` is optional but useful in config reviews and diagnostics.

## Request Matching

| Match mode | Behavior |
| --- | --- |
| `exact` | Raw SQL must equal `request.sql`. This is the default. |
| `normalized` | Whitespace is collapsed, trailing `;` is ignored, and comparison is case-insensitive. |
| `regex` | `request.sql` is compiled as a Go regular expression and matched against raw SQL. |
| `contains` | Raw SQL must contain `request.sql`. |
| `any` | SQL text is ignored. |

Omit `request.params` to ignore query parameters. When `params` is present, the
argument count must match and each value is compared by its string
representation. `[]byte` values compare as strings.

Rules are evaluated in config order. The first matching rule wins unless it has
already been consumed by `response.once: true`.

## Responses

### OK

```yaml
response:
  type: ok
  affected_rows: 1
  last_insert_id: 42
  warnings: 0
```

### Result Set

```yaml
response:
  type: result_set
  columns:
    - name: id
      type: BIGINT
    - name: email
      type: VARCHAR
  rows:
    - [1, "alice@example.com"]
```

`row_format: object` lets rows be written as maps keyed by column name:

```yaml
response:
  type: result_set
  columns:
    - name: id
      type: BIGINT
    - name: email
      type: VARCHAR
  row_format: object
  rows:
    - id: 1
      email: "alice@example.com"
```

For a single-column result set, scalar row values are accepted.

Supported rule column types include:

`CHAR`, `VARCHAR`, `TEXT`, `STRING`, `TINYINT`, `BOOL`, `BOOLEAN`, `SMALLINT`,
`INT`, `INTEGER`, `MEDIUMINT`, `BIGINT`, `FLOAT`, `DOUBLE`, `REAL`, `DECIMAL`,
`NUMERIC`, `DATE`, `NEWDATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `TINYBLOB`,
`MEDIUMBLOB`, `LONGBLOB`, `BLOB`, `BINARY`, `VARBINARY`, `JSON`, `ENUM`, `SET`,
and `GEOMETRY`.

### Error

```yaml
response:
  type: error
  code: 1062
  sql_state: "23000"
  message: "Duplicate entry for key 'users.email'"
```

Use error rules to model database behaviors mysqlmock does not emulate
internally, such as MySQL error `3572` for `FOR UPDATE NOWAIT` lock conflicts.

### Disconnect

```yaml
response:
  type: disconnect
  after_ms: 50
```

`delay_ms` waits before returning any response. `after_ms` is used by
`disconnect` to wait before closing the connection.

## Response Profiles

Profiles fill in common response fields:

| Profile | Type | Code | SQL state |
| --- | --- | --- | --- |
| `deadlock` | `error` | `1213` | `40001` |
| `lock_wait_timeout` | `error` | `1205` | `HY000` |
| `duplicate_key` | `error` | `1062` | `23000` |
| `foreign_key_violation` | `error` | `1452` | `23000` |
| `disconnect` | `disconnect` | n/a | n/a |

Example:

```yaml
rules:
  - name: first insert deadlocks
    request:
      match: contains
      sql: "INSERT INTO orders"
    response:
      profile: deadlock
      once: true
```

Explicit fields override profile defaults when they are compatible with the
profile's response type.

## Unsupported Query Diagnostics

Unsupported queries are recorded when mysqlmock cannot handle a statement through
rules, built-in compatibility handlers, or SQLite fallback. The recorded
diagnostic includes:

- raw SQL
- normalized SQL
- connection ID
- MySQL command
- current database
- route stage
- generated rule suggestion

Library tests can inspect diagnostics:

```go
unsupported := server.Unsupported()
queries := server.Queries()
stats := server.Stats()
snapshot, err := server.QuerySnapshotJSON()
unsupportedSnapshot, err := server.UnsupportedSnapshotJSON()
```

`Stats()` returns counters for routed queries, metadata work, resets, schema
changes, unsupported SQL, and aggregate timings. Timings are reported in
nanoseconds as query totals by command, route, and kind, plus fixed internal
phases such as `sqlite.query`, `sqlite.exec`, `protocol.result_set_text`,
`protocol.result_set_sqlite_text`, `information_schema.target_table_refresh`,
and `reset.data_only`. It does not store SQL text, normalized SQL, parameters,
table names, or column names.
`Server.Reset` does not clear stats; take snapshots before and after a workflow
when per-workflow counts or timings are needed.

For the common assertion that repository tests must not emit unsupported SQL,
use:

```go
mysqlmock.AssertNoUnsupported(t, server)
```

For golden-file tests, use:

```go
err := mysqlmock.WriteQuerySnapshot("testdata/queries.golden.json", server.Queries())
err = mysqlmock.WriteUnsupportedSnapshot("testdata/unsupported.golden.json", server.Unsupported())
```

Snapshot JSON omits volatile connection IDs and omits `normalized_sql` when it
is identical to `sql`. Unsupported snapshots also include generated rule
suggestions so newly observed SQL can be reviewed as a fixture.

## CLI Diagnostics

Run a config check:

```sh
mysqlmock check --config testdb.yaml
```

Print a starter rule template:

```sh
mysqlmock dump-unsupported-template
```

Fail a long-running server when unsupported SQL is observed:

```sh
mysqlmock serve --config testdb.yaml --fail-on-unsupported
```

Emit query logs:

```sh
mysqlmock serve --config testdb.yaml --verbose --log-format json
```

Print SQL-body-free execution stats as JSON to stderr when the server shuts
down:

```sh
mysqlmock serve --config testdb.yaml --print-stats
```

`--log-format` supports `text` and `json`.
