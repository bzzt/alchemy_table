defmodule AlchemyTable.Schema do
  @moduledoc """
  Allows the creation of typed Bigtable schemas.
  """
  defmacro __using__(_opt) do
    quote do
      import unquote(__MODULE__), only: :macros
    end
  end

  @doc """
  Defines a type that can be used as the value for a `Bigtable.Schema.column/2` definition.
  """
  defmacro type(do: block) do
    quote do
      var!(columns) = []
      unquote(block)

      defstruct var!(columns)

      def type do
        %__MODULE__{}
      end
    end
  end

  @doc """
  Defines a schema to be used when reading and mutating Bigtable rows.
  """

  defmacro table(name, opts, do: block) do
    instance = Bigtable.Utils.configured_instance_name()

    quote do
      alias AlchemyTable.Operations.Update
      import AlchemyTable.Schema.Table
      Module.register_attribute(__MODULE__, :families, accumulate: true)
      Module.register_attribute(__MODULE__, :promoted, accumlate: true)
      Module.register_attribute(__MODULE__, :ts_tables, accumlate: true)
      # TODO: Use a different way to identify during schema generation
      @behaviour unquote(__MODULE__)
      @key_parts unquote(opts) |> get_key_pattern!() |> build_key_parts()
      unquote(block)
      defstruct @families

      def metadata do
        %{
          name: unquote(name),
          instance: unquote(instance),
          cloned: @cloned,
          promoted: @promoted,
          ts_tables: @ts_tables,
          schema: schema()
        }
      end

      def instance do
        Bigtable.Utils.configured_instance_name()
      end

      def schema do
        %__MODULE__{}
      end

      def update(data) do
        main_key =
          build_row_key(@key_parts, data)
          |> add_ts(unquote(opts))

        main_update =
          main_key
          |> Update.update(schema(), data)

        cloned_updates =
          @cloned
          |> Enum.map(&clone_update(&1, main_key, main_update, data))

        [{unquote(name), main_update} | cloned_updates]
      end
    end
  end

  @doc """
  Defines a column family inside a `Bigtable.Schema.row/2` definition.

  The name of the family should be provided to the macro as an atom.

  The block of the macro should only contain `Bigtable.Schema.column/2` definitions.
  """
  defmacro family(name, do: block) do
    quote do
      var!(name) = unquote(name)
      var!(columns) = []
      unquote(block)
      @families {unquote(name), Map.new(var!(columns))}
    end
  end

  @doc """
  Defines a column inside a `Bigtable.Schema.family/2` definition.

  The first argument is an atom that will define the column's name.

  The second argument defines the column's type and should be one of:
  - `:integer`
  - `:float`
  - `:boolean`
  - `:string`
  - `:map`
  - `:list`

  If the column value is defined as either `:map` or `:list`, the value will be JSON encoded during mutations and decoded during reads.
  """
  defmacro column(key, {:__aliases__, _, _} = value) do
    type =
      Macro.expand(value, __CALLER__)
      |> apply(:type, [])
      |> Macro.escape()

    c = {key, type}

    quote do
      var!(columns) = [unquote(c) | var!(columns)]
    end
  end

  defmacro column(key, value) do
    c = {key, value}

    quote do
      var!(columns) = [unquote(c) | var!(columns)]
    end
  end

  defmacro ts_column(key, value) do
    quote do
      @ts_tables [[var!(name), unquote(key)]]
      column(unquote(key), unquote(value))
    end
  end

  # defmacro promoted(key, value) do
  # module = Macro.expand(value, __CALLER__)

  # base_type =
  # module
  # |> apply(:schema, [])
  # |> Map.from_struct()
  # |> Macro.escape()

  # quote do
  # @promoted [{[var!(name), unquote(key)], unquote(module)}]
  # type = unquote(base_type) |> get_in([var!(name), unquote(key)])
  # var!(columns) = [{unquote(key), type} | var!(columns)]
  # end
  # end
end
