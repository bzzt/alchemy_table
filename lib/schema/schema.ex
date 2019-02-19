defmodule AlchemyTable.Schema do
  @moduledoc """
  Allows the creation of typed Bigtable schemas.
  """

  defmacro __using__(_opt) do
    quote do
      import unquote(__MODULE__)
      Module.register_attribute(__MODULE__, :rows, accumulate: true)
      Module.register_attribute(__MODULE__, :families, accumulate: true)
      Module.register_attribute(__MODULE__, :columns, accumulate: true)

      def instance do
        Bigtable.Utils.configured_instance_name()
      end
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

  defmacro table(name, do: block) do
    quote do
      Module.register_attribute(__MODULE__, :promoted, accumlate: true)
      @behaviour unquote(__MODULE__)
      @name unquote(to_string(name))
      unquote(block)
      defstruct @families

      def metadata do
        %{
          name: @name,
          instance: instance(),
          type: type()
        }
      end

      def type do
        %__MODULE__{}
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
  defmacro column(key, {:__aliases__, _, _} = value, opts) do
    type =
      Macro.expand(value, __CALLER__)
      |> apply(:type, [])
      |> Macro.escape()

    c = {key, type}

    quote do
      var!(columns) = [unquote(c) | var!(columns)]
    end
  end

  defmacro column(key, value, _opts) do
    c = {key, value}

    quote do
      var!(columns) = [unquote(c) | var!(columns)]
    end
  end

  defmacro column(key, value) do
    quote do
      column(unquote(key), unquote(value), [])
    end
  end

  defmacro promoted(key, value) do
    base_type =
      Macro.expand(value, __CALLER__)
      |> apply(:type, [])
      |> Map.from_struct()
      |> Macro.escape()

    quote do
      type = unquote(base_type) |> get_in([var!(name), unquote(key)])
      var!(columns) = [{unquote(key), type} | var!(columns)]
    end
  end
end
