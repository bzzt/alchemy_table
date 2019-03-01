defmodule AlchemyTable.Operations.Get do
  @moduledoc false

  alias AlchemyTable.{Parsing, Table}
  alias Bigtable.ReadRows

  alias Google.Bigtable.V2.ReadRowsRequest

  def get(meta, %ReadRowsRequest{} = request) do
    full_name = Table.Utils.full_name(meta.instance, meta.table_name)

    %{request | table_name: full_name}
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
