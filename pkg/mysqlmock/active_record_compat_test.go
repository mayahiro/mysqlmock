package mysqlmock

import "testing"

func TestParseActiveRecordShowStatements(t *testing.T) {
	if !isShowFullFieldsQuery("SHOW FULL FIELDS FROM `AR_USERS`") {
		t.Fatal("SHOW FULL FIELDS was not recognized")
	}
	table, like, ok := parseShowFullFields("SHOW FULL FIELDS FROM `ar_users` LIKE 'email'")
	if !ok || table != "ar_users" || like != "email" {
		t.Fatalf("parse SHOW FULL FIELDS = table:%q like:%q ok:%v, want ar_users/email/true", table, like, ok)
	}
	table, ok = parseShowCreateTable("SHOW CREATE TABLE `ar_users`")
	if !ok || table != "ar_users" {
		t.Fatalf("parse SHOW CREATE TABLE = table:%q ok:%v, want ar_users/true", table, ok)
	}
	table, ok = parseShowKeys("SHOW KEYS FROM `ar_users`")
	if !ok || table != "ar_users" {
		t.Fatalf("parse SHOW KEYS = table:%q ok:%v, want ar_users/true", table, ok)
	}
}
