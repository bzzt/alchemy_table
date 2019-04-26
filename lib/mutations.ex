defmodule AlchemyTable.Mutations do
  @moduledoc """
  Provides functionality for generating mutations for data based on a provided schema.
  """
  alias AlchemyTable.{Encoding, Utils, Validation}
  alias Bigtable.Mutations
  alias Google.Bigtable.V2.MutateRowsRequest.Entry

  @doc """
  Creates mutations for `data` based on a provided `schema`.
  Raises if `data` contains values that do not conform to schema. Values present in `data` that are not defined in the `schema` are passed through.
  """
  def create_mutations(row_key, data, timestamp) do
    row_key
    |> Mutations.build()
    |> apply_mutations(data, mutation_timestamp(timestamp))
  end

  defp apply_mutations(entry, data, timestamp, access \\ []) do
    Enum.reduce(data, entry, fn {k, v}, accum ->
      access = [k | access]

      cond do
        is_map(v) ->
          apply_mutations(accum, v, timestamp, access)

        true ->
          mutate_cell(accum, v, timestamp, Enum.reverse(access))
      end
    end)
  end

  defp mutate_cell(entry, value, timestamp, [family | columns]) do
    family = to_string(family)
    qualifier = dot_notation(columns)

    if is_nil(value) do
      Mutations.delete_from_column(entry, family, qualifier)
    else
      Mutations.set_cell(entry, family, qualifier, value, timestamp)
    end
  end

  @spec dot_notation([atom()]) :: binary()
  defp dot_notation(columns) do
    columns
    |> Enum.map(&to_string/1)
    |> Enum.join(".")
  end

  # Builds a unix timestamp from either a datetime or ISO string
  @spec mutation_timestamp(DateTime.t() | binary()) :: integer()
  defp mutation_timestamp(timestamp) when is_binary(timestamp) do
    {:ok, datetime, _} = DateTime.from_iso8601(timestamp)
    unix_timestamp(datetime)
  end

  defp mutation_timestamp(timestamp), do: unix_timestamp(timestamp)

  # unix timestamp needs to be microseconds rounded to nearest millisecond
  # or bigtable will reject the update
  @spec unix_timestamp(DateTime.t()) :: integer()
  defp unix_timestamp(datetime) do
    ts =
      datetime
      |> DateTime.to_unix(:millisecond)

    ts * 1000
  end
end
