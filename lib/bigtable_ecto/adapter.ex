defmodule Bigtable.Ecto.Adapter do
  @moduledoc false

  alias Bigtable.Ecto.{Admin, Decoding, Encoding, Query}
  alias Bigtable.Ecto.Migration.Table

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable
  defmacro __before_compile__(_) do
  end

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
    apply(Query, function, [query, params, adapter_meta])
  end

  def execute_ddl(adapter_meta, {function, %Table{} = table, operations}, opts) do
    apply(Admin, function, [adapter_meta, table, operations, opts])
  end

  def execute_ddl(adapter_meta, {function, %Table{} = table}, opts) do
    apply(Admin, function, [adapter_meta, table, opts])
  end

  def prepare(atom, query) do
    {:nocache, {atom, query}}
  end
end
