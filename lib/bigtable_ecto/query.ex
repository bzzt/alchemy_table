defmodule AlchemyTable.Query do
  def all(%Ecto.Query{} = query, params, adapter_meta) do
    %{instance: instance, project: project} = adapter_meta

    %{from: from, wheres: wheres} = query

    IO.puts("QUERY - ALL")
    {table, model} = from.source

    IO.inspect(table)

    fields = model.__schema__(:fields)

    {:ok, _query, rows} =
      "projects/#{project}/instances/#{instance}/tables/#{table}"
      |> Bigtable.ReadRows.build()
      |> maybe_filter(wheres, params)
      |> Bigtable.ReadRows.read()

    parsed =
      rows
      |> AlchemyTable.Parsing.parse_rows()
      |> Enum.map(fn r ->
        Enum.map(fields, fn f ->
          Map.get(r, f)
        end)
      end)

    {length(parsed), parsed}
  end

  def update_all(%Ecto.Query{} = query, params, adapter_meta) do
    IO.puts("UPDATE ALL")
    %{instance: instance, project: project} = adapter_meta

    %{from: from, updates: updates, wheres: wheres} = query
    {table, _model} = from.source
    table_name = "projects/#{project}/instances/#{instance}/tables/#{table}"

    {:ok, rows} =
      table_name
      |> Bigtable.ReadRows.build()
      |> maybe_filter(wheres, params)
      |> Bigtable.ReadRows.read()

    row_keys =
      rows
      |> AlchemyTable.Parsing.parse_rows()
      |> Enum.map(&Map.fetch!(&1, :row_key))

    entry =
      updates
      |> Enum.flat_map(fn %Ecto.Query.QueryExpr{expr: expr} ->
        Enum.map(expr, fn update ->
          updates(update, params)
        end)
      end)
      |> List.first()

    row_keys
    |> Enum.map(fn k ->
      Map.put(entry, :row_key, k)
    end)
    |> Bigtable.MutateRows.build(table_name)
    |> Bigtable.MutateRows.mutate()
  end

  def delete_all(%Ecto.Query{} = query, params, adapter_meta) do
    IO.puts("DELETE ALL")
    %{instance: instance, project: project} = adapter_meta

    %{from: from, wheres: wheres} = query

    {table, _model} = from.source
    table_name = "projects/#{project}/instances/#{instance}/tables/#{table}"

    {:ok, _query, rows} =
      table_name
      |> Bigtable.ReadRows.build()
      |> maybe_filter(wheres, params)
      |> Bigtable.ReadRows.read()

    row_keys =
      rows
      |> AlchemyTable.Parsing.parse_rows()
      |> Enum.map(&Map.fetch!(&1, :row_key))

    entry =
      "ROW_KEY"
      |> Bigtable.Mutations.build()
      |> Bigtable.Mutations.delete_from_row()

    row_keys
    |> Enum.map(fn k ->
      Map.put(entry, :row_key, k)
    end)
    |> Bigtable.MutateRows.build(table_name)
    |> Bigtable.MutateRows.mutate()
  end

  defp maybe_filter(request, [], _params), do: request

  defp maybe_filter(request, wheres, params) do
    Enum.reduce(wheres, request, fn %Ecto.Query.BooleanExpr{expr: expr}, accum ->
      expr
      |> pair(params)
      |> filter(accum)
    end)
  end

  def updates({:set, keys}, params) do
    data =
      for {k, v} <- keys do
        {k, value(v, params)}
      end

    "ROW_KEY"
    |> AlchemyTable.Mutations.create_mutations(data, DateTime.utc_now())
  end

  def filter({:row_key, row_key}, request) do
    request
    |> Bigtable.RowSet.row_keys(row_key)
  end

  defp pair({:==, _, [left, right]}, params) do
    {field(left), value(right, params)}
  end

  defp value(%Ecto.Query.Tagged{} = tagged, params), do: value(tagged.value, params)
  defp value({:^, _, [index]}, params), do: Enum.at(params, index)
  defp value(value, _), do: value

  defp field({{:., _, [{:&, _, [0]}, field]}, _, []}), do: field
end
