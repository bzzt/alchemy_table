# defmodule AlchemyTable.Operations.Get do
#   @moduledoc false

#   alias Bigtable.{ReadRows, RowFilter, RowSet}

#   @spec get_all(binary()) :: [{:ok, Google.Bigtable.V2.ReadRowsResponse.t()}]
#   def get_all(row_prefix) do
#     regex = "^#{row_prefix}#\\w+"

#     ReadRows.build()
#     |> RowFilter.row_key_regex(regex)
#     |> ReadRows.read()
#   end

#   @spec get_by_id([binary()], binary()) :: [{:ok, Google.Bigtable.V2.ReadRowsResponse.t()}]
#   def get_by_id(ids, row_prefix) do
#     ids
#     |> Enum.map(fn id -> "#{row_prefix}##{id}" end)
#     |> RowSet.row_keys()
#     |> ReadRows.read()
#   end
# end
