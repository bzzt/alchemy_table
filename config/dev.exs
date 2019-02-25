use Mix.Config

config :bigtable,
  project: "project",
  instance: "instance"

config :alchemy_table,
  bigquery: [
    gen_schemas: true
  ]
