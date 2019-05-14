defmodule Bigtable.Ecto.Encoding do
  @moduledoc """
  Provides the ability to encode a value to a binary for use in a `Google.Bigtable.V2.Mutation` 
  """

  @mode Application.get_env(:alchemy_table, :value_mode, :bytes)

  @doc """
  Encodes a `value` given a `type`, the `value`, and an optional list of options.

  ## Examples
      iex> AlchemyTable.Encoding.encode(:boolean, true, [mode: :bytes])
      <<1>>

  The provided type can be one of:
  * `:boolean`
  * `:integer`
  * `:float`
  * `:list`
  * `:map`
  * `:string`

  ## Options
  * `:mode` - The type of encoding to perform. Can be either `:string` or `:bytes`. Defaults to application configured value or `:bytes`.
  """
  @spec encode(atom(), any(), list()) :: binary() | nil
  def encode(type, value, opts \\ [mode: @mode]) do
    mode = Keyword.fetch!(opts, :mode)

    if is_nil(value) do
      nil
    else
      case mode do
        :string ->
          encode_string(type, value)

        :bytes ->
          encode_bytes(type, value)
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
  defp encode_bytes(:naive_datetime, v), do: to_string(v)
  defp encode_bytes(:utc_datetime, v), do: to_string(v)
  defp encode_bytes(:id, v), do: to_string(v)
end
