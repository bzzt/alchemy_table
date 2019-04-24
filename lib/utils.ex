defmodule AlchemyTable.Utils do
  @moduledoc false

  @typedoc false
  @type nilled_map() :: %{required(atom()) => nil | nilled_map()}

  @spec nilled(any) :: nil | nilled_map()
  def nilled(val) when is_map(val) do
    for {k, v} <- from_struct(val), into: %{} do
      {k, nilled(v)}
    end
  end

  def nilled(_), do: nil

  @spec atoms_from_dots(binary()) :: [atom()]
  def atoms_from_dots(string) do
    string
    |> String.split(".")
    |> Enum.map(&String.to_atom/1)
  end

  @spec from_struct(map()) :: map()
  defp from_struct(map) do
    if Map.has_key?(map, :__struct__) do
      Map.from_struct(map)
    else
      map
    end
  end
end
