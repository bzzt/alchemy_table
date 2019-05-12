defmodule Bigtable.Ecto.Adapter do
  @moduledoc false

  alias AlchemyTable.{Decoding, Encoding}

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable
  defmacro __before_compile__(_) do
  end

  # def start_link(repo, opts) do
  #   IO.puts(
  #     "#{inspect(__MODULE__)}.start_link #{inspect(__MODULE__)}.start_link-params #{
  #       inspect(%{repo: repo, opts: opts})
  #     }"
  #   )

  #   Agent.start_link(fn -> [] end)
  # end

  def init(config) do
    instance = Keyword.get(config, :instance)
    project = Keyword.get(config, :project)

    {:ok, Bigtable.child_spec(config), %{instance: instance, project: project}}
  end

  # ADAPTER
  def checkout(meta, opts, fun) do
    apply(fun, [])
  end

  def ensure_all_started(repo, type) do
    IO.puts(
      "#{inspect(__MODULE__)}.ensure_all_started params #{inspect(%{type: type, repo: repo})}"
    )

    with {:ok, apps} <- Application.ensure_all_started(:bigtable) do
      {:ok, apps}
    end
  end

  def loaders({:embed, _} = type, _), do: [&load_embed(type, &1)]

  def loaders(t, type) do
    [
      fn v ->
        {:ok, Decoding.decode(t, v)}
      end,
      type
    ]
  end

  def load_embed(type, value) do
    Ecto.Type.load(type, value, fn
      {:embed, _} = type, value ->
        load_embed(type, value)

      type, value ->
        case Ecto.Type.cast(type, value) do
          {:ok, _} = ok ->
            ok

          _ ->
            :error
        end
    end)
  end

  def dumpers({:embed, _} = type, _) do
    [&dump_embed(type, &1)]
  end

  def dumpers(:binary_id, type) do
    [
      type,
      fn v -> {:ok, Encoding.encode(:string, v)} end
    ]
  end

  def dumpers(t, type) do
    [
      type,
      fn v -> {:ok, Encoding.encode(t, v)} end
    ]
  end

  def dump_embed(type, value) do
    Ecto.Type.dump(type, value, fn
      {:embed, _} = type, value ->
        dump_embed(type, value)

      :binary_id, value ->
        {:ok, Encoding.encode(:string, value)}

      type, value ->
        {:ok, Encoding.encode(type, value)}
    end)
  end

  # SCHEMA
  def autogenerate(_), do: nil

  def delete(adapter_meta, schema_meta, filters, options) do
    IO.puts("DELETE")
  end

  def insert(meta, %{source: source}, fields, _, _, _) do
    %{instance: instance, project: project} = meta

    {row_key, data} = Keyword.pop(fields, :row_key)

    update =
      data
      |> Map.new()

    {:ok, _query, _result} =
      row_key
      |> AlchemyTable.Mutations.create_mutations(update, DateTime.utc_now())
      |> Bigtable.MutateRow.build("projects/#{project}/instances/#{instance}/tables/" <> source)
      |> Bigtable.MutateRow.mutate()

    {:ok, []}
  end

  def insert_all(adapter_meta, schema_meta, header, list, on_conflict, returning, options) do
    IO.puts("INSERT ALL")
  end

  def update(adapter_meta, %{source: source}, fields, filters, _returning, _options) do
    %{instance: instance, project: project} = adapter_meta

    update =
      fields
      |> Map.new()

    {:ok, _} =
      filters
      |> Keyword.get(:row_key)
      |> AlchemyTable.Mutations.create_mutations(update, DateTime.utc_now())
      |> Bigtable.MutateRow.build("projects/#{project}/instances/#{instance}/tables/" <> source)
      |> Bigtable.MutateRow.mutate()

    {:ok, []}
  end

  # QUERYABLE
  def execute(adapter_meta, _query_meta, {:nocache, {function, query}}, params, options) do
    apply(AlchemyTable.Query, function, [query, params, adapter_meta])
  end

  def prepare(atom, query) do
    {:nocache, {atom, query}}
  end
end

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

  defp value({:^, _, [index]}, params), do: Enum.at(params, index)
  defp value(value, _), do: value

  defp field({{:., _, [{:&, _, [0]}, field]}, _, []}), do: field
end
