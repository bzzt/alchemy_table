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
end
