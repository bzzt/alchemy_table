defmodule AlchemyTable.Connection do
  use GenServer

  def start_link() do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    IO.puts("INSIDE CONNECTION")
    {:ok, %{}}
  end
end

defmodule AlchemyTable do
  @moduledoc false

  alias AlchemyTable.{Connection, Decoding, Encoding}

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable

  defmacro __before_compile__(_) do
  end

  def start_link(repo, opts) do
    IO.puts(
      "#{inspect(__MODULE__)}.start_link #{inspect(__MODULE__)}.start_link-params #{
        inspect(%{repo: repo, opts: opts})
      }"
    )

    Agent.start_link(fn -> [] end)
  end

  def init(config) do
    instance = Keyword.get(config, :instance)
    project = Keyword.get(config, :project)
    import Supervisor.Spec
    child_spec = worker(Connection, [])
    {:ok, child_spec, %{instance: instance, project: project}}
  end

  # ADAPTER
  def checkout(meta, opts, fun) do
    IO.puts("INSIDE CHECKOUT")
    IO.inspect(meta)
    IO.inspect(opts)
    apply(fun, [])
  end

  def ensure_all_started(repo, type) do
    IO.puts(
      "#{inspect(__MODULE__)}.ensure_all_started params #{inspect(%{type: type, repo: repo})}"
    )

    with {:ok, _} = Application.ensure_all_started(:alchemy_table) do
      {:ok, [repo]}
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

    {:ok, _} =
      row_key
      |> AlchemyTable.Mutations.create_mutations(update, DateTime.utc_now())
      |> Bigtable.MutateRow.build("projects/#{project}/instances/#{instance}/tables/" <> source)
      |> Bigtable.MutateRow.mutate()

    {:ok, []}
  end

  def insert_all(adapter_meta, schema_meta, header, list, on_conflict, returning, options) do
    IO.puts("INSERT ALL")
  end

  def update(adapter_meta, schema_meta, fields, filters, returning, options) do
    IO.puts("UPDATE")
  end

  # QUERYABLE
  def execute(adapter_meta, _query_meta, {:nocache, {function, query}}, params, options) do
    IO.puts("EXECUTE")
    apply(AlchemyTable.Query, function, [query, params, adapter_meta])
  end

  def prepare(atom, query) do
    IO.puts("PREPARE")
    {:nocache, {atom, query}}
  end
end

defmodule AlchemyTable.Query do
  def all(%Ecto.Query{} = query, params, adapter_meta) do
    %{instance: instance, project: project} = adapter_meta

    %{from: from} = query

    IO.puts("QUERY - ALL")
    {table, model} = from.source

    fields = model.__schema__(:fields)

    {:ok, rows} =
      "projects/#{project}/instances/#{instance}/tables/#{table}"
      |> Bigtable.ReadRows.build()
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
end
