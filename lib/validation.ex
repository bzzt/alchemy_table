defmodule AlchemyTable.Validation do
  @moduledoc """
  Provides functionality to validate maps based on a provided schema.
  """

  @doc """
  Validates a `map` against a provided `schema`, erroring out if `map` does not conform.

  Keys that exist in the map but not in the schema will be ignored.
  """
  @spec validate_map!(map(), map()) :: :ok
  def validate_map!(schema, map) do
    Enum.each(map, fn {k, v} ->
      if Map.get(schema, k) != nil do
        type = Map.get(schema, k)

        case typed_map?(type, v) do
          true ->
            nested_map = Map.get(map, k)
            validate_map!(type, nested_map)

          false ->
            type
            |> validate!(v, map)
        end
      else
        :ok
      end
    end)
  end

  defp validate!(nil, _, _), do: :ok

  defp validate!(type, value, parent) do
    unless valid?(type, value) do
      raise(
        RuntimeError,
        "Value #{inspect(value)} does not conform to type #{inspect(type)} in #{inspect(parent)}"
      )
    end

    :ok
  end

  defp typed_map?(type, value) when is_map(type) and is_map(value), do: true
  defp typed_map?(_, _), do: false

  def valid?(_, nil), do: true
  def valid?(:boolean, v), do: is_boolean(v)
  def valid?(:string, v), do: is_binary(v)
  def valid?(:integer, v), do: is_integer(v)
  def valid?(:float, v), do: is_float(v) or is_integer(v)
  def valid?(:list, v), do: is_list(v)
  def valid?(:map, v), do: is_map(v)
end
