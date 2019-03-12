defmodule AlchemyTable.Parsing do
  @moduledoc """
  Provides functionality for parsing the result of a `Bigtable.ChunkReader`.
  """
  alias AlchemyTable.Decoding
  alias AlchemyTable.Table.Utils

  @typedoc "Map keyed by row keys with parsed rows as values."
  @type parsed_rows() :: %{optional(binary()) => map()}

  @doc """
  Parses the result of a `Bigtable.ChunkReader` based on a provided `schema`.
  """
  @spec parse_rows(map(), map()) :: parsed_rows()
  def parse_rows(rows, metadata) do
    spec = Map.from_struct(metadata.schema)

    rows
    |> Map.new(&parse_row(metadata, &1))
  end

  defp parse_row(metadata, {row_key, chunks}) do
    schema = metadata.schema |> Map.from_struct()

    parsed =
      chunks
      |> Enum.reduce(%{}, fn chunk, accum ->
        family = String.to_atom(chunk.family_name.value)

        qualifiers =
          chunk.qualifier.value
          |> Utils.atoms_from_dots()

        access_pattern = [family | qualifiers]
        access_func = Enum.map(access_pattern, &Access.key(&1, %{}))

        type = Enum.reduce(access_pattern, schema, &Map.get(&2, &1))

        put_in(accum, access_func, Decoding.decode(type, chunk.value))
      end)

    merged = DeepMerge.deep_merge(metadata.merge_map, parsed)

    {row_key, merged}
  end
end
