defmodule AlchemyTable.Operations.Get do
  @moduledoc """
  Provides funtionaly for reading rows from a table and parsing the results based on its schema.
  """
  alias AlchemyTable.{Parsing, Table}
  alias Bigtable.ReadRows
  alias Google.Bigtable.V2.ReadRowsRequest
  alias Table.Utils

  @typedoc "Returns either parsed rows or a GRPC error"
  @type get_response() :: {:ok, map()} | {:error, any()}

  @doc """
  Submits a `Google.Bigtable.V2.ReadRowsRequest` and parses the response based on the table's schema.
  """
  @spec get(map(), ReadRowsRequest.t()) :: get_response()
  def get(%{instance: instance, table_name: table_name} = metadata, request) do
    full_name = Utils.full_name(instance, table_name)

    request
    |> Map.put(:table_name, full_name)
    |> read_and_parse(metadata)
  end

  @spec read_and_parse(ReadRowsRequest.t(), map()) :: get_response()
  defp(read_and_parse(request, metadata)) do
    with {:ok, rows} <- ReadRows.read(request),
         result <- Parsing.parse_rows(rows, metadata) do
      {:ok, result}
    else
      err ->
        err
    end
  end
end
