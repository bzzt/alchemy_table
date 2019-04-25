defmodule AlchemyTable.Mutations do
  @moduledoc """
  Provides functionality for generating mutations for data based on a provided schema.
  """
  alias AlchemyTable.{Encoding, Validation, Utils}
  alias Google.Bigtable.V2.MutateRowsRequest.Entry

  @doc """
  Creates mutations for `data` based on a provided `schema`.

  Raises if `data` contains values that do not conform to schema. Values present in `data` that are not defined in the `schema` are passed through.
  """
  @spec create_mutations(binary(), map(), map(), binary()) :: Entry.t()
  def create_mutations(row_key, schema, data, timestamp) do
    Validation.validate_update!(schema, data)
    entry = Bigtable.Mutations.build(row_key)

    Enum.reduce(data, entry, fn {family_name, columns}, accum ->
      case Map.get(schema, family_name) do
        # Ignore values that don't exist in the schema. Will make this behavior an option eventually.
        nil ->
          accum

        type ->
          apply_mutations(type, columns, accum, to_string(family_name), timestamp)
      end
    end)
  end

  # Applys mutations to the Entry based on the schema.
  @spec apply_mutations(map(), map(), Entry.t(), binary(), binary(), binary() | nil) :: Entry.t()
  defp apply_mutations(schema, data, entry, family_name, timestamp, parent_key \\ nil) do
    Enum.reduce(data, entry, fn {k, v}, accum ->
      column_qualifier = column_qualifier(parent_key, k)

      case Map.get(schema, k) do
        # Ignore values that don't exist in the schema. Will make this behavior an option eventually.
        nil ->
          accum

        # Recursively applies mutations to nested maps
        type when is_map(type) ->
          nested_map(type, v, accum, family_name, column_qualifier, timestamp)

        type ->
          encoded = Encoding.encode(type, v)

          accum
          |> add_cell_mutation(family_name, column_qualifier, encoded, timestamp)
      end
    end)
  end

  # Adds a mutation to the entry. Will either set the cell value or delete the cell
  # Depending on if the value is nil or not
  @spec add_cell_mutation(Entry.t(), binary(), binary(), nil | binary(), binary() | DateTime.t()) ::
          Entry.t()
  defp add_cell_mutation(accum, family_name, column_qualifier, nil, _) do
    accum
    |> Bigtable.Mutations.delete_from_column(family_name, column_qualifier)
  end

  defp add_cell_mutation(accum, family_name, column_qualifier, value, timestamp) do
    datetime =
      if is_binary(timestamp) do
        {:ok, datetime, _} =
          timestamp
          |> DateTime.from_iso8601()

        datetime
      else
        timestamp
      end

    unix_timestamp =
      datetime
      |> DateTime.to_unix(:millisecond)

    accum
    |> Bigtable.Mutations.set_cell(family_name, column_qualifier, value, unix_timestamp * 1000)
  end

  # If a value is a nested map, recursively apply mutations with either the map value
  # or a map of nil values if the value is nil
  @spec nested_map(map(), binary() | nil, Entry.t(), binary(), binary(), binary()) :: Entry.t()
  defp nested_map(type, value, accum, family_name, column_qualifier, timestamp) do
    if value == nil or value == "" do
      niled_map = Utils.nilled(type)
      apply_mutations(type, niled_map, accum, family_name, timestamp, column_qualifier)
    else
      apply_mutations(type, value, accum, family_name, timestamp, column_qualifier)
    end
  end

  # Builds up a dot notation column qualifier if the current column has a parent key
  @spec column_qualifier(binary() | nil, atom()) :: binary()
  defp column_qualifier(parent_key, key) do
    case parent_key do
      nil -> to_string(key)
      parent -> "#{parent}.#{to_string(key)}"
    end
  end
end
