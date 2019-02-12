defmodule AlchemyTable.Decoding do
  @mode Application.get_env(:alchemy_table, :value_mode, :bytes)

  def decode(type, v, mode \\ @mode) do
    case mode do
      :string ->
        decode_string(type, v)

      :bytes ->
        decode_bytes(type, v)
    end
  end

  defp decode_string(:integer, v), do: Integer.parse(v)
  defp decode_string(:list, v), do: Poison.decode!(v)
  defp decode_string(:map, v), do: Poison.decode!(v)
  defp decode_string(:float, v), do: Float.parse(v)
  defp decode_string(:boolean, "false"), do: false
  defp decode_string(:boolean, "true"), do: true
  defp decode_string(:string, v), do: v

  defp decode_bytes(:boolean, v) do
    case v do
      <<0, 0, 0, 0, 0, 0, 0, 1>> -> true
      <<0, 0, 0, 0, 0, 0, 0, 0>> -> false
    end
  end

  defp decode_bytes(:integer, v) do
    <<int::integer-signed-64>> = v
    int
  end

  defp decode_bytes(:float, v) do
    <<float::signed-float-64>> = v
    float
  end

  defp decode_bytes(:map, v), do: Poison.decode!(v)
  defp decode_bytes(:list, v), do: Poison.decode!(v)
  defp decode_bytes(:string, v), do: v
end
