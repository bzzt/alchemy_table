defmodule AlchemyTable.Schema do
  alias AlchemyTable.{Operations, Reads}
  alias Operations.{Delete, Get, Update}

  @moduledoc """
  Allows the creation of typed Bigtable schemas.

  ## Examples
      iex> defmodule SchemaExample do
      ...>  use AlchemyTable.Schema
      ...>  @update_patterns ["family_a.column_a"]
      ...>  row :entity do
      ...>    family :family_a do
      ...>      column(:column_a, :integer)
      ...>      column(:column_b, :boolean)
      ...>    end
      ...>    family :family_b do
      ...>      column(:column_a, :map)
      ...>      column(:column_b, :list)
      ...>    end
      ...>  end
      ...> end
      iex> SchemaExample.type() |> Map.from_struct()
      %{
        family_a: %{
          column_a: :integer,
          column_b: :boolean
        },
        family_b: %{
          column_a: :map,
          column_b: :list
        }
      }
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

  ## Examples
  ```elixir

  defmodule Type do
    use Bigtable.Schema

    type do
      column(:a, :integer)
      column(:b, :string)
    end
  end

  defmodule Schema do
    use Bigtable.Schema

    row :entity do
      family :family do
        column(:column, Type)
      end
    end
  end
  ```
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

  ## Examples
  ```elixir

  defmodule Schema do
    use Bigtable.Schema

    row :entity do
      family :family_a do
        column(:column_a, :string)
      end

      family :family_b do
        column(:column_a, :integer)
        column(:column_b, :boolean)
      end
    end
  end
  ```
  """

  defmacro table(name, do: block) do
    quote do
      @behaviour unquote(__MODULE__)
      @name unquote(to_string(name))
      @prefix "#{String.capitalize(to_string(unquote(name)))}"
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
  defmacro column(key, value) do
    c = {key, get_value_type(value)} |> Macro.escape()

    quote do
      var!(columns) = [unquote(c) | var!(columns)]
    end
  end

  defp get_value_type(value) when is_atom(value), do: value

  defp get_value_type({:__aliases__, _, modules}) do
    Module.concat([Elixir | modules]).type()
  end
end

defmodule BT.Schema.PositionTest do
  @moduledoc false

  use AlchemyTable.Schema

  type do
    column(:bearing, :integer)
    column(:latitude, :float)
    column(:longitude, :float)
    column(:timestamp, :string)
  end
end

defmodule BT.Schema.VehicleTest do
  @moduledoc false

  use AlchemyTable.Schema

  @update_patterns ["vehicle.id"]

  table :vehicle do
    family :vehicle do
      column(:battery, :integer)
      column(:checkedInAt, :string)
      column(:condition, :string)
      column(:driver, :string)
      column(:fleet, :string)
      column(:id, :string)
      column(:numberPlate, :string)
      column(:position, BT.Schema.PositionTest)
      column(:previousPosition, BT.Schema.PositionTest)
      column(:ride, :string)
    end
  end
end
