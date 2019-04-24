defmodule AlchemyTable.Parsing do
  @moduledoc """
  Provides functionality for parsing the result of a `Bigtable.ChunkReader`.
  """
  alias AlchemyTable.{Decoding, Utils}
  alias Bigtable.ChunkReader.ReadCell

  @doc """
  Parses the result of a `Bigtable.ChunkReader` based on a provided `schema`.
  """
  @spec parse_rows(map(), map()) :: map()
  def parse_rows(rows, metadata) do
    %{schema: schema, merge_map: merge_map} = metadata

    for {row_key, chunks} <- rows, into: %{} do
      parsed =
        chunks
        |> parse_chunks(schema)
        |> merge_with_defaults(merge_map)

      {row_key, parsed}
    end
  end

  # Merges the parsed row with the schema's default values
  # Defaults to a map full of nils
  @spec merge_with_defaults(map(), map()) :: map()
  defp merge_with_defaults(parsed, merge_map), do: DeepMerge.deep_merge(merge_map, parsed)

  # Parse the row's chunks using the defined schema
  @spec parse_chunks([ReadCell.t()], map()) :: map()
  defp parse_chunks(chunks, schema) do
    Enum.reduce(chunks, %{}, fn chunk, accum ->
      # build an access function to fetch the chunk's type from
      # the schema and put the value into the return map
      access_func = build_access_function(chunk)

      ## Decode the cell's value based on the defined schema
      decoded =
        schema
        |> get_in(access_func)
        |> Decoding.decode(chunk.value)

      # assign the decoded value to the accum using access func
      put_in(accum, access_func, decoded)
    end)
  end

  @spec build_access_function(ReadCell.t()) :: [function()]
  defp build_access_function(chunk) do
    family = family_atom(chunk)

    qualifier_keys = qualifier_atoms(chunk)

    access_keys = [family | qualifier_keys]

    # build an access function for each key
    # the access function returns an empty map if the property
    # does not exist in the provided map
    for key <- access_keys do
      Access.key(key, %{})
    end
  end

  # Returns an atom from the chunk's column family
  @spec family_atom(ReadCell.t()) :: atom()
  defp family_atom(chunk), do: String.to_atom(chunk.family_name.value)

  # Returns list of atoms to use as an access pattern.
  # Extracts keys from the convention of nested maps being stored
  # with dot notation column qualifiers such as `first.second.third`
  @spec qualifier_atoms(ReadCell.t()) :: [atom()]
  defp qualifier_atoms(chunk), do: Utils.atoms_from_dots(chunk.qualifier.value)
end
