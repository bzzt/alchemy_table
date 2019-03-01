defmodule AlchemyTable.Operations.Update do
  @moduledoc false
  alias AlchemyTable.{Mutations, Table, Validation}
  alias Bigtable.MutateRow

  # @spec mutations_from_maps(map(), [map()], binary(), [binary()]) ::
  #         Google.Bigtable.V2.MutateRows.Entry.t()
  # def mutations_from_maps(type_spec, maps, row_prefix, update_patterns) do
  #   Enum.each(maps, &Validation.validate_map!(type_spec, &1))

  #   mutations = Enum.map(maps, &mutations_from_map(type_spec, &1, row_prefix, update_patterns))

  #   mutations
  #   |> List.flatten()
  #   |> MutateRows.build()
  # end
  # def update(row_key, type_spec, map) do
  #   Validation.validate_map!(type_spec, map)
  #   Mutations.create_mutations(row_key, type_spec, map)
  # end

  def update(module, data, timestamp) do
    build_updates(module, data, timestamp)
    |> Enum.map(&build_mutate_row/1)
  end

  defp build_updates(module, data, timestamp) do
    %{
      cloned: cloned,
      promoted: promoted,
      instance: instance,
      schema: schema,
      key_parts: key_parts,
      opts: opts,
      table_name: table_name
    } = module.__alchemy_metadata__()

    main_key =
      Table.Utils.build_row_key(key_parts, data)
      |> add_ts(opts, timestamp)

    main_update =
      main_key
      |> build_update(schema, data)

    cloned_updates =
      for table <- List.flatten(cloned), into: [] do
        meta = table.__alchemy_metadata__()
        %{name: table_name, instance: instance, opts: opts} = meta
        update = clone_update(main_key, main_update, data, opts, timestamp)
        {instance, table_name, update}
      end

    promoted_updates =
      for {column, module} <- promoted,
          get_in(data, column) != nil,
          into: [] do
        build_updates(module, data, timestamp)
      end

    [{instance, table_name, main_update}, cloned_updates, promoted_updates]
    |> List.flatten()
  end

  defp build_update(row_key, schema, data) do
    Validation.validate_map!(schema, data)
    Mutations.create_mutations(row_key, schema, data)
  end

  defp clone_update(main_key, main_update, data, opts, timestamp) do
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
