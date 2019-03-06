defmodule AlchemyTable.Operations.Get do
  @moduledoc """
  Provides funtionaly for reading rows from a table and parsing the results based on its schema.
  """
  alias AlchemyTable.{Parsing, Table}
  alias Bigtable.ReadRows
  alias Google.Bigtable.V2.ReadRowsRequest

  @typedoc "Returns either parsed rows or a GRPC error"
  @type get_response() :: {:ok, Parsing.parsed_rows()} | {:error, any()}

  @doc """
  Submits a `Google.Bigtable.V2.ReadRowsRequest` and parses the response based on a table's schema.
  """
  @spec get(map(), ReadRowsRequest.t()) :: get_response()
  def get(meta, request) do
    full_name = Table.Utils.full_name(meta.instance, meta.table_name)

    request
    |> Map.put(:table_name, full_name)
    |> read_and_parse(meta.schema)
  end

  @spec read_and_parse(ReadRowsRequest.t(), map()) :: get_response()
  defp(read_and_parse(request, schema)) do
    with {:ok, rows} <- ReadRows.read(request),
         result <- Parsing.parse_rows(rows, schema) do
      {:ok, result}
    else
      err ->
        err
    end
  end
end
