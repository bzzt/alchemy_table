defmodule AlchemyTable.Encoding do
  @mode Application.get_env(:alchemy_table, :value_mode, :bytes)
  def encode(type, v, mode \\ @mode) do
    if is_nil(v) do
      nil
    else
      case mode do
        :string ->
          encode_string(type, v)

        :bytes ->
          encode_bytes(type, v)
      end
    end
  end

  defp encode_string(:list, v), do: Poison.encode!(v)
  defp encode_string(:map, v), do: Poison.encode!(v)
  defp encode_string(_, v), do: to_string(v)

  defp encode_bytes(:boolean, true), do: <<1>>
  defp encode_bytes(:boolean, false), do: <<0>>
  defp encode_bytes(:integer, v), do: <<v::integer-signed-64>>
  defp encode_bytes(:float, v), do: <<v::signed-float-64>>
  defp encode_bytes(:list, v), do: Poison.encode!(v)
  defp encode_bytes(:map, v), do: Poison.encode!(v)
  defp encode_bytes(:string, v), do: to_string(v)
end
