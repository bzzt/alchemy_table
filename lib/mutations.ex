defmodule AlchemyTable.Mutations do
  @moduledoc """
  Provides functionality for generating mutations for data based on a provided schema.
  """
  alias AlchemyTable.Encoding
  alias AlchemyTable.Validation
  alias Google.Bigtable.V2.MutateRowsRequest.Entry

  @doc """
  Creates mutations for `data` based on a provided `schema`. Errors out if the data does not conform to the schema.
  """
  @spec create_mutations(binary(), map(), map()) :: Entry.t()
  def create_mutations(row_key, schema, data) do
    Validation.validate_map!(schema, data)
    entry = Bigtable.Mutations.build(row_key)

    Enum.reduce(data, entry, fn {k, v}, accum ->
      case Map.get(schema, k) do
        nil ->
          accum

        type ->
          apply_mutations(type, v, accum, to_string(k))
      end
    end)
  end

  # Applys mutations to the Entry based on the schema.
  @spec apply_mutations(map(), map(), Entry.t(), binary(), binary() | nil) :: Entry.t()
  defp apply_mutations(schema, data, entry, family_name, parent_key \\ nil) do
    Enum.reduce(data, entry, fn {k, v}, accum ->
      column_qualifier = column_qualifier(parent_key, k)

      case Map.get(schema, k) do
        # Ignore values that don't exist in the schema. Will make this behavior an option eventually.
        nil ->
          accum

        # Recursively applies mutations to nested maps
        type when is_map(type) ->
          nested_map(type, v, accum, family_name, column_qualifier)

        type ->
          encoded = Encoding.encode(type, v)

          accum
          |> add_cell_mutation(family_name, column_qualifier, encoded)
      end
    end)
  end

  # Adds a mutation to the entry. Will either set the cell value or delete the cell
  # Depending on if the value is nil or not
  @spec add_cell_mutation(Entry.t(), binary(), binary(), nil | binary()) :: Entry.t()
  defp add_cell_mutation(accum, family_name, column_qualifier, nil) do
    accum
    |> Bigtable.Mutations.delete_from_column(family_name, column_qualifier)
  end

  defp add_cell_mutation(accum, family_name, column_qualifier, value) do
    accum
    |> Bigtable.Mutations.set_cell(family_name, column_qualifier, value)
  end

  # If a value is a nested map, recursively apply mutations with either the map value
  # or a map of nil values if the value is nil
  @spec nested_map(map(), binary() | nil, Entry.t(), binary(), binary()) :: Entry.t()
  defp nested_map(type, value, accum, family_name, column_qualifier) do
    if value == nil or value == "" do
      niled_map = nil_values(type)
      apply_mutations(type, niled_map, accum, family_name, column_qualifier)
    else
      apply_mutations(type, value, accum, family_name, column_qualifier)
    end
  end

  @typedoc "Map where all values are either `nil` or a `nil_map`"
  @type nil_map() :: %{required(atom()) => nil | nil_map()}

  # Returns a map of nil values
  @spec nil_values(map()) :: nil_map()
  defp nil_values(schema) do
    Enum.reduce(schema, %{}, fn {k, v}, accum ->
      if is_map(v) do
        Map.put(accum, k, nil_values(v))
      else
        Map.put(accum, k, nil)
      end
    end)
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
