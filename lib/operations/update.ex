defmodule AlchemyTable.Operations.Update do
  @moduledoc false
  alias AlchemyTable.{Mutations, Table}
  alias Bigtable.{MutateRow, RowSet}

  def update(module, data, opts) do
    updates = build_updates(module, data, opts)

    updates
    |> Enum.map(&send_update(&1, opts))
    |> Map.new()
  end

  defp build_updates(module, data, opts) do
    meta = module.__alchemy_metadata__()

    timestamp = Keyword.fetch!(opts, :timestamp)

    with row_key <- build_row_key(meta, data, timestamp),
         mutations <- main_mutations(meta, row_key, data),
         cloned_mutations <- cloned_mutations(meta, row_key, mutations, data, timestamp),
         promoted_mutations <- promoted_mutations(meta, data, opts) do
      [{module, mutations}, cloned_mutations, promoted_mutations]
      |> List.flatten()
    end
  end

  defp build_row_key(%{key_parts: key_parts, opts: opts}, data, timestamp) do
    row_key = Table.Utils.build_row_key(key_parts, data)

    row_key
    |> add_ts(opts, timestamp)
  end

  defp main_mutations(%{schema: schema}, main_key, data) do
    main_key
    |> Mutations.create_mutations(schema, data)
  end

  defp promoted_mutations(%{promoted: promoted}, data, opts) do
    for {column, module} <- promoted,
        has_key?(nil, column, data),
        into: [] do
      build_updates(module, data, opts)
    end
  end

  defp has_key?(false, _keys, _data), do: false
  defp has_key?(result, [], _data), do: result

  defp has_key?(_result, [h | t], data) do
    has_key?(Map.has_key?(data, h), t, Map.get(data, h))
  end

  defp cloned_mutations(%{cloned: cloned}, main_key, main_update, data, timestamp) do
    for module <- List.flatten(cloned), into: [] do
      meta = module.__alchemy_metadata__()
      mutations = clone_mutations(main_key, main_update, data, meta.opts, timestamp)
      {module, mutations}
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

  defp send_update({module, mutations}, opts) do
    %{instance: instance, table_name: table_name} = module.__alchemy_metadata__()
    full_name = Table.Utils.full_name(instance, table_name)

    with {:ok, _} <- mutate_row(mutations, full_name) do
      response = build_response(module, mutations.row_key, opts)
      {table_name, response}
    else
      err ->
        {table_name, err}
    end
  end

  defp mutate_row(mutations, table_name) do
    mutations
    |> MutateRow.build(table_name)
    |> MutateRow.mutate()
  end

  defp build_response(module, row_key, opts) do
    if Keyword.get(opts, :return, false) do
      row_key = RowSet.row_keys(row_key)

      row_key
      |> module.get()
    else
      :ok
    end
  end
end
