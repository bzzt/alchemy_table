defmodule AlchemyTable.Validation do
  @moduledoc false

  @doc """
  Validates an update against a provided `schema`, raising if the update is invalid.

  Keys that exist in the map but not in the schema will be ignored.
  """
  def validate_update!(schema, update, parent \\ nil)

  def validate_update!(schema, update, _parent) when is_map(update) and schema != :map do
    Enum.each(update, fn {k, v} ->
      case Map.get(schema, k) do
        nil ->
          :ok

        type ->
          validate_update!(type, v, update)
      end
    end)
  end

  def validate_update!(schema, update, parent) do
    validate!(schema, update, parent)
  end

  defp validate!(type, value, parent) do
    unless valid?(type, value) do
      raise(
        RuntimeError,
        "Value #{inspect(value)} does not conform to type #{inspect(type)} in #{inspect(parent)}"
      )
    end
  end

  def valid?(nil, _), do: true
  def valid?(_, nil), do: true
  def valid?(:boolean, v), do: is_boolean(v)
  def valid?(:string, v), do: is_binary(v)
  def valid?(:integer, v), do: is_integer(v)
  def valid?(:float, v), do: is_float(v) or is_integer(v)
  def valid?(:list, v), do: is_list(v)
  def valid?(:map, v), do: is_map(v)
end
