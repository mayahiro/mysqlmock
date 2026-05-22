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
```

## CLI

```sh
go run ./cmd/mysqlmock serve --config testdb.yaml --listen 127.0.0.1:0 --print-dsn
go run ./cmd/mysqlmock check --config testdb.yaml
```

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
- No TLS, compression, `multiStatements=true`, or `LOAD DATA LOCAL INFILE`.
- MySQL-specific SQL compatibility is intentionally small and will be expanded
  from real unsupported-query reports.
