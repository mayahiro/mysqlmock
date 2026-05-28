# Basic Repository Test Example

This example shows a small repository-style test using `mysqlmock` with
`github.com/go-sql-driver/mysql`.

Run it from the repository root:

```sh
go test ./examples/basic
```

The test starts a mysqlmock server from `config.yaml`, opens a real MySQL driver
connection, runs CRUD through a small repository type, and calls `Server.Reset`
to restore configured schema and seed data.
