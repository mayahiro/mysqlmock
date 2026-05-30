# Rules と Diagnostics

mysqlmock は各 SQL statement を次の順序で routing します

```text
1. rules
2. built-in compatibility handlers
3. 小さな MySQL-to-SQLite translation を伴う SQLite fallback
4. unsupported query error
```

Rules を使うと、特定 SQL の上書き、固定 result set、MySQL-like error の注入、遅延、client connection の切断を設定できます

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

`name` は任意ですが、config review や diagnostics で役に立ちます

## Request Matching

| Match mode | 挙動 |
| --- | --- |
| `exact` | raw SQL が `request.sql` と完全一致する必要があります（default） |
| `normalized` | whitespace を collapse し、末尾の `;` を無視し、case-insensitive に比較します |
| `regex` | `request.sql` を Go regular expression として compile し、raw SQL に match します |
| `contains` | raw SQL が `request.sql` を含む必要があります |
| `any` | SQL text を無視します |

`request.params` を省略すると query parameter は無視されます
`params` がある場合は argument count が一致し、各値が string representation で比較されます
`[]byte` は string として比較されます

Rules は config の順序で評価されます
最初に match した rule が使われます
ただし `response.once: true` で消費済みの rule は次回以降 skip されます

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

`row_format: object` を使うと、column name を key にした map として row を書けます

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

1 column の result set では、scalar row value も使えます

対応する rule column type は次の通りです

`CHAR`、`VARCHAR`、`TEXT`、`STRING`、`TINYINT`、`BOOL`、`BOOLEAN`、`SMALLINT`、
`INT`、`INTEGER`、`MEDIUMINT`、`BIGINT`、`FLOAT`、`DOUBLE`、`REAL`、`DECIMAL`、
`NUMERIC`、`DATE`、`NEWDATE`、`TIME`、`DATETIME`、`TIMESTAMP`、`TINYBLOB`、
`MEDIUMBLOB`、`LONGBLOB`、`BLOB`、`BINARY`、`VARBINARY`、`JSON`、`ENUM`、`SET`、
`GEOMETRY`

### Error

```yaml
response:
  type: error
  code: 1062
  sql_state: "23000"
  message: "Duplicate entry for key 'users.email'"
```

mysqlmock が内部で再現しない database behavior は error rule で表現できます
たとえば `FOR UPDATE NOWAIT` の lock conflict は MySQL error `3572` として model 化できます

### Disconnect

```yaml
response:
  type: disconnect
  after_ms: 50
```

`delay_ms` は任意の response を返す前に待ちます
`after_ms` は `disconnect` で connection を閉じる前の待ち時間です

## Response Profiles

Profile は common response fields を補完します

| Profile | Type | Code | SQL state |
| --- | --- | --- | --- |
| `deadlock` | `error` | `1213` | `40001` |
| `lock_wait_timeout` | `error` | `1205` | `HY000` |
| `duplicate_key` | `error` | `1062` | `23000` |
| `foreign_key_violation` | `error` | `1452` | `23000` |
| `disconnect` | `disconnect` | n/a | n/a |

例:

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

明示した field は、profile の response type と互換性がある場合に profile default を上書きします

## Unsupported Query Diagnostics

Unsupported query は、rules、built-in compatibility handlers、SQLite fallback のいずれでも statement を扱えなかった場合に記録されます
記録される diagnostics には次が含まれます

- raw SQL
- normalized SQL
- connection ID
- MySQL command
- current database
- route stage
- generated rule suggestion

Library test では diagnostics を取得できます

```go
unsupported := server.Unsupported()
queries := server.Queries()
stats := server.Stats()
snapshot, err := server.QuerySnapshotJSON()
unsupportedSnapshot, err := server.UnsupportedSnapshotJSON()
```

`Stats()` は routed query、metadata work、reset、schema change、unsupported SQL の counter を返します
SQL 本文、normalized SQL、parameters、table names、column names は保持しません
`Server.Reset` は stats を消さないため、workflow 単位の件数が必要な場合は前後の snapshot を比較してください

Repository test が unsupported SQL を出していないことを確認する標準 assertion には次を使えます

```go
mysqlmock.AssertNoUnsupported(t, server)
```

Golden-file test では次を使えます

```go
err := mysqlmock.WriteQuerySnapshot("testdata/queries.golden.json", server.Queries())
err = mysqlmock.WriteUnsupportedSnapshot("testdata/unsupported.golden.json", server.Unsupported())
```

Snapshot JSON は volatile な connection ID を省略します
また、`normalized_sql` が `sql` と同じ場合は省略します
Unsupported snapshot には generated rule suggestion も含まれるため、新しく観測された SQL を fixture として確認できます

## CLI Diagnostics

Config check を実行します

```sh
mysqlmock check --config testdb.yaml
```

Rule template を出力します

```sh
mysqlmock dump-unsupported-template
```

Unsupported SQL を観測した時点で long-running server を失敗させます

```sh
mysqlmock serve --config testdb.yaml --fail-on-unsupported
```

Query log を出力します

```sh
mysqlmock serve --config testdb.yaml --verbose --log-format json
```

server shutdown 時に SQL 本文を含まない実行 stats JSON を stderr に出力します

```sh
mysqlmock serve --config testdb.yaml --print-stats
```

`--log-format` は `text` と `json` に対応しています
