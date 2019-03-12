defmodule AlchemyTable.Utils do
  @moduledoc false

  @typedoc false
  @type nilled_map() :: %{required(atom()) => nil | nilled_map()}

  @spec nil_map(map()) :: nilled_map()
  def nil_map(map) do
    map
    |> from_struct()
    |> Enum.reduce(%{}, fn {k, v}, accum ->
      if is_map(v) do
        Map.put(accum, k, nil_map(v))
      else
        Map.put(accum, k, nil)
      end
    end)
  end

  defp from_struct(map) do
    if Map.has_key?(map, :__struct__) do
      Map.from_struct(map)
    else
      map
    end
  end
end
