# 設定リファレンス

mysqlmock の設定ファイルは YAML です
`mysqlmock.WithConfig` を使う場合は、同じ構造を `mysqlmock.Config` として渡せます

`ConfigFile` または `LoadConfigFile` で YAML を読み込む場合、未知の field は error になります
top-level の `version`、`server`、`database` は必須です

## 最小設定

```yaml
version: 1
server:
  auth:
    mode: allow_any
database:
  engine: sqlite
  mode: memory
```

省略された nested value には default が適用されます

## Top-Level Sections

| Field | 必須 | 説明 |
| --- | --- | --- |
| `version` | Yes | Config version、対応値は `1` のみです |
| `server` | Yes | Listener と MySQL compatibility settings |
| `database` | Yes | SQLite backend settings |
| `schema` | No | seed data の前に適用する SQL statements |
| `schema_files` | No | inline `schema` の前に適用する SQL dump files |
| `seed` | No | schema setup 後に insert する table rows |
| `seed_files` | No | inline `seed` の前に insert する YAML、JSON、CSV seed files |
| `seed_file_configs` | No | file ごとの CSV option を持つ external seed files |
| `compat` | No | Built-in MySQL compatibility variables と profiles |
| `rules` | No | SQL override と fault-injection rules |
| `fallback` | No | rules と built-in compatibility handlers の後の挙動 |

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

- `listen` は TCP listen address です。port `0` は OS に空き port を割り当てさせます
- `mysql_version` は handshake で advertise され、version compatibility handler の戻り値にも使われます
- `connection_id_start` は最初の MySQL connection ID を制御します
- `auth.mode` は現在 `allow_any` のみ対応です。client から送られた user/password を検証せず接続を許可します

## Database

```yaml
database:
  engine: sqlite
  mode: memory
  shared: true
```

`engine` は現在 `sqlite` のみ対応です

`mode: memory` は in-memory SQLite backend を作ります
`shared: true` の場合、client connection は同じ in-memory database を共有します
`shared: false` の場合、MySQL client connection ごとに、設定された `schema` と `seed` から初期化された独立した in-memory database を使います

`mode: file` は SQLite database を disk に保持し、`path` が必須です

```yaml
database:
  engine: sqlite
  mode: file
  path: "testdata/mysqlmock.sqlite"
```

File mode は、mysqlmock server の再起動をまたいで state を保持する必要がある test に限って使ってください

## Schema

`schema` は SQL statement の ordered list です

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

`schema_files` は SQL dump file の ordered list です
config file 内の path は config file の directory からの相対 path として解決されます
file は inline `schema` statements の前に適用されます

```yaml
schema_files:
  - db/schema.sql
```

mysqlmock は dump file を SQL statements に分割し、`DROP TABLE`、`CREATE TABLE`、`CREATE INDEX`、`ALTER TABLE` などの schema DDL を適用します
`SET`、`USE`、`LOCK TABLES`、`UNLOCK TABLES`、data `INSERT` などの dump 補助 statement は無視します
可能であれば `mysqldump --no-data`、または TiDB Dumpling の `*-schema.sql` file を使ってください

各 statement は、適用前に mysqlmock の小さな MySQL-to-SQLite translator を通ります
translator は Repository test でよく出る `AUTO_INCREMENT`、TiDB `AUTO_RANDOM`、TiDB clustered-index comment、`TRUE`、`FALSE`、`NOW()`、`CURRENT_TIMESTAMP(n)`、common table options、`AUTO_RANDOM_BASE`、table-level `PRIMARY KEY` / `UNIQUE KEY` / `KEY` 定義、単純な MySQL index DDL、よく使う `ALTER TABLE` / `RENAME TABLE` variants を扱います
`AUTO_INCREMENT` column が複合 primary key に含まれる場合、mysqlmock は複合 key を維持して `AUTO_INCREMENT` を削除します。SQLite が rowid を自動採番できるのは単一の `INTEGER PRIMARY KEY` のみなので、この column は seed data または test insert で明示してください
MySQL-visible index name は table scoped として扱い、SQLite 内部では private index name に変換するため、schema dump 内の複数 table で同じ index name を使えます
完全な MySQL parser の実装は目標にしていません

## Seed Data

`seed` は table name から rows への map です

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

Seed data はすべての schema statement 適用後に insert されます
`seed_files` は external seed file の ordered list です
config file 内の path は config file の directory からの相対 path として解決されます
file は inline `seed` rows の前に insert されます

```yaml
seed_files:
  - testdata/users.yaml
  - testdata/posts.json
  - testdata/tags.csv
```

YAML と JSON seed file は、`seed` と同じ table map をそのまま持つか、`seed` key 配下に包めます
CSV seed file は `.csv` を除いた file name を table name として使い、1 行目を column name、`\N` を `NULL` として扱います

file ごとの設定が必要な場合は `seed_file_configs` を使います

```yaml
seed_file_configs:
  - path: testdata/legacy_users.csv
    format: csv
    table: users
    null_values: ["NULL", "\\N"]
    infer_types: true
```

`table` は CSV file name 由来の table name を上書きします
`null_values` の default は `["\\N"]` です
`infer_types: true` は simple boolean、integer、float、common datetime string を insert 前に変換します

mysqlmock が seed insert statement を組み立てるとき、table name と column name は quote されます

## Compatibility Variables

```yaml
compat:
  profile: gorm
  variables:
    lower_case_table_names: "1"
    time_zone: "SYSTEM"
```

`default` profile には次のような common variables が含まれます

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

`gorm` profile は、ORM initialization でよく使われる次の variables を追加します

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

明示した `compat.variables` は profile default を上書きします
ただし、実効 `version` variable は常に `server.mysql_version` から決まります

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

`fallback.type` は現在 `sqlite` のみ対応です
`fallback.unsupported` は、rules、built-in compatibility handlers、SQLite fallback のいずれでも扱えなかった query に返す MySQL error を制御します

SQLite fallback は Repository test 向けの限定的な互換性として、`VALUES(column)`、ActiveRecord-style row alias、insert-side `DEFAULT` values を含む `INSERT ... ON DUPLICATE KEY UPDATE`、`INSERT IGNORE`、`REPLACE INTO` を扱います
ORM や repository query でよく使う scalar function/operator として `RAND`、`FIND_IN_SET`、`FIELD`、`REGEXP` operator も登録します
`REGEXP` は Go regular expression を使うため、MySQL regular expression dialect との完全一致は保証しません
`RAND(seed)` は同じ seed に対して deterministic ですが、MySQL の per-statement random sequence behavior までは再現しません

MySQL-compatible DDL で index を作成した場合、mysqlmock は軽量な index metadata も保持し、ORM schema introspection が使う `SHOW KEYS` の prefix length、expression、visibility fields を返します

`SHOW CREATE TABLE` が要求された場合、mysqlmock は対象 table が runtime で変更されていない間、設定で読み込んだ original MySQL/TiDB `CREATE TABLE` statement を返します
table-altering DDL 後は cached original DDL を invalidation し、MySQL-style table option を付けた現在の SQLite definition に fallback します

## JSON Schema

Config JSON Schema は次の command で出力できます

```sh
mysqlmock dump-config-schema
```

Source checkout から実行する場合:

```sh
go run ./cmd/mysqlmock dump-config-schema
```

Config が読み込めること、schema と seed data を SQLite に適用できることを確認するには次を使います

```sh
mysqlmock check --config testdb.yaml
```
