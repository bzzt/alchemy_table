defmodule AlchemyTable.Parsing do
  @moduledoc """
  Provides functionality for parsing the result of a `Bigtable.ChunkReader`.
  """
  alias AlchemyTable.Decoding

  @typedoc "Map keyed by row keys with parsed rows as values."
  @type parsed_rows() :: %{optional(binary()) => map()}

  @doc """
  Parses the result of a `Bigtable.ChunkReader` based on a provided `schema`.
  """
  @spec parse_rows(map(), map()) :: parsed_rows()
  def parse_rows(rows, schema) do
    spec = Map.from_struct(schema)

    rows
    |> Map.new(&parse_row(spec, &1))
  end

  defp parse_row(spec, {row_key, chunks}) do
    parsed =
      chunks
      |> Enum.reduce(%{}, fn chunk, accum ->
        family = String.to_atom(chunk.family_name.value)

        qualifiers =
          chunk.qualifier.value
          |> String.split(".")
          |> Enum.map(&String.to_atom/1)

        access_pattern = [family | qualifiers]
        access_func = Enum.map(access_pattern, &Access.key(&1, %{}))

        type = Enum.reduce(access_pattern, spec, &Map.get(&2, &1))

        put_in(accum, access_func, Decoding.decode(type, chunk.value))
      end)

    {row_key, parsed}
  end
end
