# ActiveRecord Smoke Check

This example is an opt-in smoke script for Rails ActiveRecord's MySQL adapter.
It is meant to be run through `MYSQLMOCK_CLIENT_COMPAT_COMMANDS` when Ruby,
`activerecord`, and `mysql2` are already available in the local environment.

```sh
MYSQLMOCK_CLIENT_COMPAT_COMMANDS='[
  {
    "name": "active_record_smoke",
    "command": ["ruby", "examples/active_record_smoke/active_record_smoke.rb"]
  }
]' go test ./pkg/mysqlmock -run TestMultiLanguageClientCompatibilityCommands
```

The script reads the mysqlmock connection details from the environment variables
that the compatibility test provides.
