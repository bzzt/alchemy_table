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
  @spec create_mutations(binary(), map(), map(), any()) :: Entry.t()
  def create_mutations(row_key, schema, data, timestamp) do
    Validation.validate_update!(schema, data)

    row_key
    |> Mutations.build()
    |> apply_mutations(schema, data, mutation_timestamp(timestamp))
  end

  @spec apply_mutations(Entry.t(), atom() | map(), any(), integer(), [atom()]) :: Entry.t()
  defp apply_mutations(entry, schema, data, timestamp, access \\ []) do
    Enum.reduce(data, entry, fn {k, v}, accum ->
      type = Map.get(schema, k)
      access = [k | access]

      cond do
        # pass through value if it does not exist in schema
        is_nil(type) ->
          accum

        # if type is a map but value is nil, recurse with a map of nilled values
        is_map(type) and empty_value?(v) ->
          apply_mutations(accum, type, Utils.nilled(type), timestamp, access)

        # Recurse for typed maps
        is_map(type) ->
          apply_mutations(accum, type, v, timestamp, access)

        true ->
          mutate_cell(accum, type, v, timestamp, Enum.reverse(access))
      end
    end)
  end

  @spec mutate_cell(Entry.t(), atom(), any(), integer(), [atom()]) :: Entry.t()
  defp mutate_cell(entry, type, value, timestamp, [family | columns]) do
    family = to_string(family)
    qualifier = dot_notation(columns)

    if is_nil(value) do
      Mutations.delete_from_column(entry, family, qualifier)
    else
      encoded = Encoding.encode(type, value)
      Mutations.set_cell(entry, family, qualifier, encoded, timestamp)
    end
  end

  @spec dot_notation([atom()]) :: binary()
  defp dot_notation(columns) do
    columns
    |> Enum.map(&to_string/1)
    |> Enum.join(".")
  end

  @spec empty_value?(any()) :: boolean()
  defp empty_value?(v), do: is_nil(v) or v == ""

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
