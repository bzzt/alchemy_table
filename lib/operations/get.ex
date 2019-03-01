defmodule AlchemyTable.Operations.Get do
  @moduledoc false

  alias AlchemyTable.{Parsing, Table}
  alias Bigtable.ReadRows

  alias Google.Bigtable.V2.RowFilter

  def get(meta, opts) do
    full_name = Table.Utils.full_name(meta.instance, meta.table_name)

    full_name
    |> ReadRows.build()
    |> apply_filter(Keyword.get(opts, :filter))
    |> read_and_parse(meta.schema)
  end

  defp apply_filter(request, nil), do: request

  defp apply_filter(request, %RowFilter{} = filter) do
    %{request | filter: filter}
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
