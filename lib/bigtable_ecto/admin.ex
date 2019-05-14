defmodule Bigtable.Ecto.Admin do
  alias Bigtable.Admin.{Table, TableAdmin}
  alias Bigtable.Ecto.Migration

  def create(adapter_meta, %Migration.Table{} = table, operations, _opts) do
    %{instance: instance, project: project} = adapter_meta
    %{name: name, prefix: prefix} = table

    parent = "projects/#{project}/instances/#{instance}"

    table =
      operations
      |> column_families
      |> table()

    execute!(:create_table, [table, table_name(prefix, name), [parent: parent]])
  end

  def drop(adapter_meta, %Migration.Table{} = table, _opts) do
    %{instance: instance, project: project} = adapter_meta
    %{name: name, prefix: prefix} = table

    table_name = "projects/#{project}/instances/#{instance}/tables/#{table_name(prefix, name)}"

    execute!(:delete_table, [table_name])
  end

  defp execute!(request, args) do
    case apply(TableAdmin, request, args) do
      {:ok, _query, result} ->
        result

      {:error, error} ->
        raise(error)

      {:error, error, _} ->
        raise(error)
    end
  end

  defp column_families(operations) do
    Map.new(operations, fn {:add, name, gc_rule, _opts} ->
      {to_string(name), gc_rule}
    end)
  end

  defp table(column_families) do
    Table.build(column_families)
  end

  defp table_name(nil, name), do: table_name(name)
  defp table_name(prefix, name), do: table_name(prefix) <> "_" <> table_name(name)
  defp table_name(name) when is_atom(name), do: table_name(Atom.to_string(name))
  defp table_name(name), do: name
end
