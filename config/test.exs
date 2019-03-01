use Mix.Config

config :alchemy_table,
  value_mode: :string

config :bigtable,
  project: "dev",
  instance: "dev",
  table: "test",
  endpoint: "localhost:9035",
  ssl: false

config :goth,
  disabled: true
