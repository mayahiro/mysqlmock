# mysqlmock

`mysqlmock` は、Go の Repository 層テスト向けの軽量な MySQL protocol test server です
MySQL client driver から接続でき、よく使う MySQL query を処理し、状態を持つ CRUD は SQLite backend で実行します

Docker や外部 MySQL server を起動せずに、MySQL client 接続を使うテストを速く実行するためのツールです
本番 DB の代替ではなく、MySQL 完全互換も目標にしていません

## 現在のスコープ

現在の実装は主に次を対象にしています

- Go `database/sql`
- `github.com/go-sql-driver/mysql`
- `PingContext`
- `QueryContext` と `ExecContext`
- `PrepareContext` と prepared statement 実行
- `BeginTx`、`Commit`、`Rollback`、savepoint

Prepared statement は、Repository test でよく使う scalar parameter type を対象にしています
`NULL`、符号付き/符号なし整数、boolean、string、bytes、float、double、string として encode された `time.Time`、`NEWDATE`、`ENUM`、`SET`、blob variants などの MySQL binary protocol alias を扱います
`interpolateParams=true` は任意です

`v0.1.0` の compatibility scope は次の通りです

| 領域 | 状態 |
| --- | --- |
| Go version | Go 1.25 以上 |
| Go SQL client | `database/sql` と `github.com/go-sql-driver/mysql` |
| Core operations | Ping、query、exec、prepared statement、transaction、savepoint |
| Schema setup | inline schema、SQL dump file、よく使う MySQL/TiDB DDL translation |
| Seed data | inline rows と YAML、JSON、CSV seed files |
| ORM behavior | よく使う GORM setup variables と ActiveRecord-style schema introspection |
| MySQL/TiDB comparison | real MySQL または TiDB に対する optional compatibility scenario |
| 対象外 | 完全な MySQL SQL parser、optimizer behavior、本物の row lock、TLS、compression、`multiStatements=true`、`LOAD DATA LOCAL INFILE` |

## インストール

Go module に library を追加します

```sh
go get github.com/mayahiro/mysqlmock/pkg/mysqlmock
```

CLI をインストールします

```sh
go install github.com/mayahiro/mysqlmock/cmd/mysqlmock@latest
```

mysqlmock には Go 1.25 以上が必要です
正確な module Go version は `go.mod` に定義されています

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

`Server.DSN()` は次のような `go-sql-driver/mysql` 向け DSN を返します

```text
user:password@tcp(127.0.0.1:<port>)/mysqlmock?interpolateParams=true&charset=utf8mb4&parseTime=true
```

完全な Repository test の例は [examples/basic](examples/basic) を参照してください

## 設定

設定ファイルは YAML です。top-level の `version`、`server`、`database` は必須です
その他の top-level section は任意です

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

fallback:
  type: sqlite
  unsupported:
    type: error
    code: 1105
    sql_state: "HY000"
    message: "Unsupported query"
```

Schema と query fallback では、Repository test でよく出る MySQL SQL を SQLite で実行できるように小さな変換を行います
`database.mode: memory` は in-memory SQLite backend を使います
`schema_files` を使うと、inline `schema` statements の前に SQL dump file から DDL を読み込めます
`seed_files` を使うと、inline `seed` rows の前に YAML、JSON、CSV file から seed rows を読み込めます
CSV file で明示的な table name、custom NULL marker、基本的な型推論が必要な場合は `seed_file_configs` を使います
`database.shared: false` にすると、MySQL client connection ごとに schema と seed data から初期化された独立した in-memory database を使います
`database.mode: file` と `database.path` を設定すると、mysqlmock server の再起動をまたいで SQLite database を保持できます

設定の詳細は [docs/configuration_ja.md](docs/configuration_ja.md) を参照してください

## SQL Rules と Diagnostics

`rules` は、mysqlmock が built-in compatibility handler や SQLite backend を使う前に、matching した SQL の応答を上書きします
対応する match mode は `exact`、`normalized`、`regex`、`contains`、`any` です
対応する response type は `ok`、`result_set`、`error`、`disconnect` です

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

Fault injection 向けに、`response.profile` で MySQL-like error や disconnect behavior を展開できます
対応 profile は `deadlock`、`lock_wait_timeout`、`duplicate_key`、`foreign_key_violation`、`disconnect` です

Unsupported query diagnostics には、raw SQL、normalized SQL、connection ID、current database、route stage、生成された rule snippet が含まれます
Query event は golden-file test 向けの安定した JSON として出力できます

詳細は [docs/rules-and-diagnostics_ja.md](docs/rules-and-diagnostics_ja.md) を参照してください
Repository test では workflow 後に `mysqlmock.AssertNoUnsupported(t, server)` を呼び、case 間では `server.Reset(ctx)` で schema、seed data、auto-increment state、rules、diagnostics を戻す使い方を推奨します

## 互換性メモ

Built-in compatibility handler は、MySQL client や ORM の初期化でよく出る query を扱います
対象には `SET NAMES`、`SET autocommit`、`SELECT VERSION()`、`SELECT @@...`、`SHOW VARIABLES`、`SHOW TABLES`、ActiveRecord-style schema introspection query、小さな `information_schema` subset が含まれます

Built-in scalar compatibility function として、`DATABASE()`、`SCHEMA()`、`USER()`、`CURRENT_USER()`、`CONNECTION_ID()`、`LAST_INSERT_ID()`、`ROW_COUNT()` を扱います

`information_schema.schemata`、`tables`、`columns`、`key_column_usage`、`statistics`、`table_constraints`、`referential_constraints`、`check_constraints` は、SQLite schema から派生した小さな metadata subset として利用できます

ActiveRecord-style schema introspection として、`SHOW FULL FIELDS`、`SHOW CREATE TABLE`、`SHOW KEYS` を扱います
`SHOW CREATE TABLE` は table が変更されていない間は設定で読み込んだ original MySQL/TiDB DDL を優先し、table-altering DDL 後は runtime SQLite definition に fallback します
`GET_LOCK` と `RELEASE_LOCK` などの advisory lock function は、connection が保持する単純な lock conflict を再現します
`SHOW KEYS` は、mysqlmock の MySQL-compatible DDL path で作成された index について prefix length、expression、visibility metadata も返します

Write validation は、duplicate key、foreign key、NOT NULL、CHECK constraint、character column の data too long、incorrect integer value、incorrect datetime value など、Repository test でよく使う失敗を MySQL-like error に map します

Schema と query fallback は、`TRUE`、`FALSE`、`NOW()`、`CURRENT_TIMESTAMP()`、`AUTO_INCREMENT`、TiDB `AUTO_RANDOM`、よく使われる MySQL/TiDB DDL option、table-level `PRIMARY KEY` / `UNIQUE KEY` / `KEY` 定義、単純な MySQL index DDL、よく使う `ALTER TABLE` / `RENAME TABLE` variants を、可能な範囲で SQLite-compatible SQL に変換します
MySQL-visible index name は table scoped のまま扱い、SQLite 内部では private index name に変換することで SQLite の schema-wide index namespace との衝突を避けます
ORM query でよく使う scalar function として `IFNULL`、`COALESCE`、`CONCAT`、`CAST`、`DATE_FORMAT`、`JSON_EXTRACT`、`JSON_UNQUOTE` を扱います

SQLite fallback は、`VALUES(column)`、ActiveRecord-style row alias、insert-side `DEFAULT` values を含む一般的な `INSERT ... ON DUPLICATE KEY UPDATE`、`INSERT IGNORE`、`REPLACE INTO` と、`NOWAIT` / `SKIP LOCKED` を含む `FOR UPDATE` locking clause の strip も扱います
mysqlmock は本物の MySQL row lock は再現しません

## CLI

インストール済み CLI では次のように使います

```sh
mysqlmock serve --config testdb.yaml --listen 127.0.0.1:0 --print-dsn
mysqlmock serve --config testdb.yaml --verbose --log-format json
mysqlmock serve --config testdb.yaml --fail-on-unsupported
mysqlmock check --config testdb.yaml
mysqlmock dump-unsupported-template
mysqlmock dump-config-schema
```

Source checkout から実行する場合は、`mysqlmock` を `go run ./cmd/mysqlmock` に置き換えてください

`serve --fail-on-unsupported` は unsupported SQL を観測した時点で error exit します
`--verbose --log-format=json` は route-aware query log を JSON Lines として出力します
`dump-config-schema` は config file 向け JSON Schema を出力します

## ドキュメント

- [設定リファレンス](docs/configuration_ja.md)
- [Rules と Diagnostics](docs/rules-and-diagnostics_ja.md)
- [v0.1.1 Release Notes](docs/releases/v0.1.1_ja.md)
- [v0.1.0 Release Notes](docs/releases/v0.1.0_ja.md)
- [Architecture](ARCHITECTURE.md)
- [Basic Example](examples/basic)
- [English README](README.md)

## 開発

```sh
make fmt
make vet
make test
make build
```

`MYSQLMOCK_REAL_MYSQL_DSN` を設定すると、実際の MySQL database と CRUD、transaction、duplicate-key、upsert、`INSERT IGNORE`、`REPLACE INTO` behavior を比較する optional compatibility scenario を実行できます

```sh
MYSQLMOCK_REAL_MYSQL_DSN='user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true' \
  go test ./pkg/mysqlmock -run TestRealMySQLCompatibilityScenario
```

`MYSQLMOCK_REAL_TIDB_DSN` を設定すると、同じ scenario を TiDB に対して実行できます

```sh
MYSQLMOCK_REAL_TIDB_DSN='user:password@tcp(127.0.0.1:4000)/testdb?parseTime=true' \
  go test ./pkg/mysqlmock -run TestRealTiDBCompatibilityScenario
```

`MYSQLMOCK_CLIENT_COMPAT_COMMANDS` には、他言語 client の opt-in compatibility check 用 external client command を JSON array として指定できます
各 command は shell を使わずに実行され、`MYSQLMOCK_HOST`、`MYSQLMOCK_PORT`、`MYSQLMOCK_USER`、`MYSQLMOCK_PASSWORD`、`MYSQLMOCK_DATABASE`、`MYSQLMOCK_ADDR`、`MYSQLMOCK_DSN` を environment variable として受け取ります
Ruby dependencies が利用できる環境では、この hook で Rails / ActiveRecord の smoke script を実行する想定です
[examples/active_record_smoke](examples/active_record_smoke) に例があります

## 既知の制限

- Prepared statement support は、すべての MySQL binary protocol type の網羅を目標にしていません
- `SET NAMES` は connection character set variable を記録しますが、query data や result data の transcoding は行いません
- TLS、compression、`multiStatements=true`、`LOAD DATA LOCAL INFILE` は未対応です
- MySQL-specific SQL compatibility は意図的に小さく保っており、実際の unsupported-query report から拡張する前提です

## License

MIT
詳細は [LICENSE](LICENSE) を参照してください
