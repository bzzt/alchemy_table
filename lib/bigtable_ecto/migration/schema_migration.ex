defmodule Bigtable.Ecto.Migration.SchemaMigration do
  # Defines a schema that works with a table that tracks schema migrations.
  # The table name defaults to `schema_migrations`.
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query, only: [from: 2]

  defmodule Migration do
    use Ecto.Schema
    @primary_key false

    embedded_schema do
      field(:version, :integer)
      timestamps(updated_at: false)
    end
  end

  @primary_key false
  schema "schema_migrations" do
    field(:row_key, :integer)
    embeds_one(:migration, Migration)
  end

  @opts [timeout: :infinity, log: false]

  def ensure_schema_migrations_table!(repo, prefix) do
    table_name = repo |> get_source

    %{instance: instance, project: project} = Ecto.Adapter.lookup_meta(repo)

    parent = "projects/#{project}/instances/#{instance}"

    case Bigtable.Admin.TableAdmin.get_table("#{parent}/tables/#{table_name}") do
      {:error, _error} ->
        {:ok, _query, result} =
          %{
            "migration" => Bigtable.Admin.GcRule.max_num_versions(1)
          }
          |> Bigtable.Admin.Table.build()
          |> Bigtable.Admin.TableAdmin.create_table(table_name, parent: parent)

        result

      {:ok, _query, result} ->
        result
    end
  end

  def versions(repo, prefix) do
    from(p in get_source(repo), select: p.row_key)
    |> Map.put(:prefix, prefix)
  end

  def up(repo, version, prefix) do
    %__MODULE__{row_key: version, migration: %{version: version}}
    |> Ecto.put_meta(prefix: prefix, source: get_source(repo))
    |> repo.insert!(@opts)
  end

  def down(repo, version, prefix) do
    from(p in get_source(repo), where: p.row_key == type(^version, :integer))
    |> Map.put(:prefix, prefix)
    |> repo.delete_all(@opts)
  end

  def get_source(repo) do
    Keyword.get(repo.config, :migration_source, "schema_migrations")
  end
end