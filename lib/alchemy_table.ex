defmodule AlchemyTable.Connection do
  use GenServer

  def start_link() do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    {:ok, %{}}
  end
end

defmodule AlchemyTable do
  @moduledoc false

  alias AlchemyTable.{Decoding, Encoding}

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema

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
    IO.inspect(config)
    import Supervisor.Spec
    child_spec = worker(__MODULE__.Connection, [])
    {:ok, child_spec, %{}}
  end

  # ADAPTER
  def checkout(meta, opts, fun) do
    IO.inspect(meta)
    IO.inspect(opts)
    IO.inspect(fun)
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
        IO.puts("FO")
        load_embed(type, value)

      type, value ->
        IO.puts("FOFOFOFOFOF")

        case Ecto.Type.cast(type, value) do
          {:ok, _} = ok ->
            IO.inspect("HERE")
            ok

          _ ->
            IO.inspect("HERE2")
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

  def insert(_adapter_meta, %{source: source}, fields, on_conflict, returning, options) do
    AlchemyTable.Mutations.create_mutations("VEHICLE#123", fields, DateTime.utc_now())

    {:ok, []}
  end

  def insert_all(adapter_meta, schema_meta, header, list, on_conflict, returning, options) do
    IO.puts("INSERT ALL")
  end

  def update(adapter_meta, schema_meta, fields, filters, returning, options) do
    IO.puts("UPDATE")
  end
end
