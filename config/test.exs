use Mix.Config

config :alchemy_table,
  value_mode: :string

config :bigtable,
  project: "dev",
  instance: "dev"

config :goth,
  disabled: true
