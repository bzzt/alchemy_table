defmodule AlchemyTable.Operations.Update do
  @moduledoc false
  alias AlchemyTable.{Mutations, Table}
  alias Bigtable.MutateRow

  def update(module, data, timestamp) do
    build_updates(module, data, timestamp)
    |> Enum.map(&build_mutate_row/1)
  end

  defp build_updates(module, data, timestamp) do
    meta = module.__alchemy_metadata__()

    with row_key <- build_row_key(meta, data, timestamp),
         mutations <- main_mutations(meta, row_key, data),
         cloned_mutations <- cloned_mutations(meta, row_key, mutations, data, timestamp),
         promoted_mutations <- promoted_mutations(meta, data, timestamp) do
      [{meta.instance, meta.table_name, mutations}, cloned_mutations, promoted_mutations]
      |> List.flatten()
    end
  end

  defp build_row_key(%{key_parts: key_parts, opts: opts}, data, timestamp) do
    Table.Utils.build_row_key(key_parts, data)
    |> add_ts(opts, timestamp)
  end

  defp main_mutations(%{schema: schema}, main_key, data) do
    main_key
    |> Mutations.create_mutations(schema, data)
  end

  defp promoted_mutations(%{promoted: promoted}, data, timestamp) do
    for {column, module} <- promoted,
        get_in(data, column) != nil,
        into: [] do
      build_updates(module, data, timestamp)
    end
  end

  defp cloned_mutations(%{cloned: cloned}, main_key, main_update, data, timestamp) do
    for table <- List.flatten(cloned), into: [] do
      meta = table.__alchemy_metadata__()
      %{name: table_name, instance: instance, opts: opts} = meta
      mutations = clone_mutations(main_key, main_update, data, opts, timestamp)
      {instance, table_name, mutations}
    end
  end

  defp clone_mutations(main_key, main_update, data, opts, timestamp) do
    key =
      case Table.Utils.get_key_pattern(opts) do
        nil ->
          main_key

        key ->
          key
          |> Table.Utils.build_key_parts()
          |> Table.Utils.build_row_key(data)
      end

    key = key |> add_ts(opts, timestamp)

    %{main_update | row_key: key}
  end

  defp add_ts(key, opts, timestamp) do
    ts_suffix =
      if Keyword.get(opts, :ts, false) do
        "##{timestamp}"
      else
        ""
      end

    key <> ts_suffix
  end

  def build_mutate_row({instance, table, mutations}) do
    table_name = to_string(table) |> Recase.to_kebab()

    mutations
    |> MutateRow.build("#{instance}/tables/#{table_name}")
  end
end
