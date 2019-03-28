defmodule AlchemyTable.Operations.Get do
  @moduledoc """
  Provides funtionaly for reading rows from a table and parsing the results based on its schema.
  """
  alias AlchemyTable.Parsing
  alias Bigtable.ReadRows
  alias Google.Bigtable.V2.ReadRowsRequest

  @typedoc "Returns either parsed rows or a GRPC error"
  @type get_response() :: {:ok, Parsing.parsed_rows()} | {:error, any()}

  @doc """
  Submits a `Google.Bigtable.V2.ReadRowsRequest` and parses the response based on a table's schema.
  """
  @spec get(map(), ReadRowsRequest.t()) :: get_response()
  def get(%{full_name: full_name} = metadata, request) do
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
