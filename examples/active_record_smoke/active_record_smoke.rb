# frozen_string_literal: true

require "active_record"

ActiveRecord::Base.establish_connection(
  adapter: "mysql2",
  host: ENV.fetch("MYSQLMOCK_HOST"),
  port: ENV.fetch("MYSQLMOCK_PORT").to_i,
  username: ENV.fetch("MYSQLMOCK_USER"),
  password: ENV.fetch("MYSQLMOCK_PASSWORD"),
  database: ENV.fetch("MYSQLMOCK_DATABASE"),
  encoding: "utf8mb4"
)

connection = ActiveRecord::Base.connection

connection.create_table(:active_record_smoke_users, force: true) do |t|
  t.string :email, null: false
  t.string :name, null: false, default: "anonymous"
  t.integer :login_count, null: false, default: 0
  t.timestamps null: true
end
connection.add_index :active_record_smoke_users, :email, unique: true

class ActiveRecordSmokeUser < ActiveRecord::Base
  self.table_name = "active_record_smoke_users"
end

ActiveRecordSmokeUser.create!(email: "alice@example.com", name: "Alice")
ActiveRecordSmokeUser.upsert_all(
  [
    { email: "alice@example.com", name: "Alice Updated", login_count: 2 },
    { email: "bob@example.com", name: "Bob", login_count: 1 }
  ],
  unique_by: :email
)

raise "missing upserted row" unless ActiveRecordSmokeUser.find_by!(email: "alice@example.com").name == "Alice Updated"
raise "schema introspection failed" unless connection.columns(:active_record_smoke_users).any? { |column| column.name == "email" }
raise "index introspection failed" unless connection.indexes(:active_record_smoke_users).any? { |index| index.columns == ["email"] }

connection.drop_table(:active_record_smoke_users)
