# mysqlmock

`mysqlmock` is a lightweight MySQL-protocol test server for Go repository tests.
It accepts connections from MySQL client drivers and executes stateful CRUD
queries against an in-memory SQLite backend.

## MVP-0 Scope

The current implementation targets:

- Go `database/sql`
- `github.com/go-sql-driver/mysql`
- `PingContext`
- `QueryContext` and `ExecContext`
- `PrepareContext` and prepared statement execution
- `BeginTx`, `Commit`, and `Rollback`

Prepared statements are supported for the common scalar parameter types used by
Go repository tests. `interpolateParams=true` is optional:

```text
user:password@tcp(127.0.0.1:<port>)/mysqlmock?interpolateParams=true&charset=utf8mb4&parseTime=true
```

## Library Usage

```go
server := mysqlmock.Start(t, mysqlmock.ConfigFile("testdb.yaml"))

db, err := sql.Open("mysql", server.DSN())
if err != nil {
    t.Fatal(err)
}
defer db.Close()

// Reset restores the configured schema and seed data for another test step.
if err := server.Reset(context.Background()); err != nil {
    t.Fatal(err)
}
```

## Config Example

```yaml
version: 1

server:
  mysql_version: "8.0.36-mock"
  auth:
    mode: allow_any

database:
  engine: sqlite
  mode: memory

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

fallback:
  type: sqlite
  unsupported:
    type: error
    code: 1105
    sql_state: "HY000"
    message: "Unsupported query"
```

## SQL Rules

`rules` can override matching SQL before mysqlmock uses built-in compatibility
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
      type: error
      code: 1062
      sql_state: "23000"
      message: "Duplicate entry for key 'users.email'"
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

## CLI

```sh
go run ./cmd/mysqlmock serve --config testdb.yaml --listen 127.0.0.1:0 --print-dsn
go run ./cmd/mysqlmock serve --config testdb.yaml --verbose --log-format json
go run ./cmd/mysqlmock check --config testdb.yaml
```

Use `serve --fail-on-unsupported` to exit with an error after shutdown if the
server observed unsupported SQL. The error includes generated rule snippets that
can be copied into the config and adjusted. Unsupported query diagnostics include
the raw SQL, normalized SQL, connection ID, current database, and route stage.
Use `--verbose --log-format=json` to emit route-aware query logs as JSON Lines.

## Development

```sh
make fmt
make vet
make test
make build
```

## Known Limitations

- Prepared statement support does not aim to cover every MySQL binary protocol
  type yet.
- `SET NAMES` records connection character set variables but does not transcode
  query or result data.
- No TLS, compression, `multiStatements=true`, or `LOAD DATA LOCAL INFILE`.
- MySQL-specific SQL compatibility is intentionally small and will be expanded
  from real unsupported-query reports.
