defmodule AlchemyTable.Operations.Get do
  @moduledoc false

  alias AlchemyTable.{Parsing, Table}
  alias Bigtable.ReadRows

  def get(meta, request) do
    full_name = Table.Utils.full_name(meta.instance, meta.table_name)

    request
    |> Map.put(:table_name, full_name)
    |> read_and_parse(meta.schema)
  end

  defp read_and_parse(request, schema) do
    {:ok, rows} =
      request
      |> ReadRows.read()

    result =
      rows
      |> Parsing.parse_rows(schema)

    {:ok, result}
  end
end
