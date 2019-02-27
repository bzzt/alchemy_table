defmodule AlchemyTable.Decoding do
  @mode Application.get_env(:alchemy_table, :value_mode, :bytes)

  def decode(type, v, opts \\ [mode: @mode]) do
    mode = Keyword.fetch!(opts, :mode)
    opts = Keyword.put_new(opts, :keys, :atoms)

    case mode do
      :string ->
        decode_string(type, v, opts)

      :bytes ->
        decode_bytes(type, v, opts)
    end
  end

  defp decode_string(:integer, v, _opts) do
    Integer.parse(v)
    |> elem(0)
  end

  defp decode_string(:float, v, _opts) do
    Float.parse(v)
    |> elem(0)
  end

  defp decode_string(:list, v, opts), do: decode_json(v, opts)
  defp decode_string(:map, v, opts), do: decode_json(v, opts)

  defp decode_string(:boolean, "false", _opts), do: false
  defp decode_string(:boolean, "true", _opts), do: true
  defp decode_string(:string, v, _opts), do: v

  defp decode_bytes(:boolean, v, _opts) do
    case v do
      <<1>> -> true
      <<0>> -> false
    end
  end

  defp decode_bytes(:integer, v, _opts) do
    <<int::integer-signed-64>> = v
    int
  end

  defp decode_bytes(:float, v, _opts) do
    <<float::float-signed-64>> = v
    float
  end

  defp decode_bytes(:map, v, opts), do: decode_json(v, opts)
  defp decode_bytes(:list, v, opts), do: decode_json(v, opts)
  defp decode_bytes(:string, v, _opts), do: v

  defp decode_json(json, opts) do
    keys = Keyword.fetch!(opts, :keys)
    Poison.decode!(json, keys: keys)
  end
end
