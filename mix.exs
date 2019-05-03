defmodule AlchemyTable.MixProject do
  use Mix.Project

  @version "0.1.3"

  def project do
    [
      app: :alchemy_table,
      version: @version,
      package: package(),
      elixir: "~> 1.7",
      name: "Alchemy Table",
      homepage_url: "https://github.com/bzzt/alchemy_table",
      source_url: "https://github.com/bzzt/alchemy_table",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      aliases: aliases(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        "coveralls.json": :test,
        "coveralls.html": :test,
        coverage: :test
      ]
    ]
  end

  defp package() do
    [
      description: "Opinionated schema management for Bigtable and BigQuery.",
      maintainers: ["Jason Scott"],
      licenses: ["MIT"],
      links: %{github: "https://github.com/bzzt/alchemy_table"}
    ]
  end

  defp docs do
    [
      source_ref: "v#{@version}",
      formatters: ["html", "epub"]
    ]
  end

  defp aliases do
    [
      coverage: [
        "coveralls.json"
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:bigtable, git: "https://github.com/bzzt/bigtable", branch: "develop"},
      {:bigtable, path: "../bigtable"},
      {:ecto, "~> 3.1.2"},
      {:deep_merge, "~> 0.1.1"},
      {:recase, "~> 0.4"},
      # Dev deps
      {:credo, "~> 1.0.0", only: [:dev, :test, :ci], runtime: false},
      {:dialyxir, "~> 1.0.0-rc.6", only: [:dev], runtime: false},
      {:excoveralls, "~> 0.10", only: [:dev, :test, :ci]},
      {:ex_doc, "~> 0.19", only: :dev, runtime: false},
      {:mix_test_watch, "~> 0.8", only: :dev, runtime: false}
    ]
  end
end
