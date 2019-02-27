defmodule AlchemyTable.Parsing do
  alias AlchemyTable.Decoding

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
