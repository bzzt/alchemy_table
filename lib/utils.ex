defmodule AlchemyTable.Utils do
  @moduledoc false

  @typedoc false
  @type nilled_map() :: %{required(atom()) => nil | nilled_map()}

  @doc """
  Replaces a value with nil or changes all of a map's values to nil.
  """
  @spec nilled(any) :: nil | nilled_map()
  def nilled(val) when is_map(val) do
    for {k, v} <- from_struct(val), into: %{} do
      {k, nilled(v)}
    end
  end

  def nilled(_), do: nil

  @doc """
  Returns a list of atoms given the convention of dot notation column qualifiers.
  foo.bar.baz -> [:foo, :bar, :baz]. Used to create access functions.
  """
  @spec atoms_from_dots(binary()) :: [atom()]
  def atoms_from_dots(string) do
    string
    |> String.split(".")
    |> Enum.map(&String.to_atom/1)
  end

  # Returns either the provided map or a map from a provided struct
  @spec from_struct(map()) :: map()
  defp from_struct(map) do
    if Map.has_key?(map, :__struct__) do
      Map.from_struct(map)
    else
      map
    end
  end
end
