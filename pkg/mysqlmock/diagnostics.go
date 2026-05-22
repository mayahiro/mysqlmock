package mysqlmock

// UnsupportedTemplate returns a YAML rule template for unsupported queries.
func UnsupportedTemplate() string {
	return "rules:\n" +
		"  - name: generated unsupported query\n" +
		"    request:\n" +
		"      match: exact\n" +
		"      sql: \"SELECT @@example\"\n" +
		"    response:\n" +
		"      type: error\n" +
		"      code: 1105\n" +
		"      sql_state: HY000\n" +
		"      message: \"Unsupported query\""
}
