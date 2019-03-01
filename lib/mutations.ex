defmodule AlchemyTable.Mutations do
  alias AlchemyTable.Encoding

  def create_mutations(row_key, type_spec, map) do
    entry = Bigtable.Mutations.build(row_key)

    Enum.reduce(map, entry, fn {k, v}, accum ->
      case Map.get(type_spec, k) do
        nil ->
          accum

        type ->
          apply_mutations(type, v, accum, to_string(k))
      end
    end)
  end

  defp apply_mutations(type_spec, map, entry, family_name, parent_key \\ nil) do
    Enum.reduce(map, entry, fn {k, v}, accum ->
      column_qualifier = column_qualifier(parent_key, k)

      case Map.get(type_spec, k) do
        nil ->
          accum

        type when is_map(type) ->
          nested_map(type, v, accum, family_name, column_qualifier)

        type ->
          encoded = Encoding.encode(type, v)

          accum
          |> add_cell_mutation(family_name, column_qualifier, encoded)
      end
    end)
  end

  defp add_cell_mutation(accum, family_name, column_qualifier, nil) do
    accum
    |> Bigtable.Mutations.delete_from_column(family_name, column_qualifier)
  end

  defp add_cell_mutation(accum, family_name, column_qualifier, value) do
    accum
    |> Bigtable.Mutations.set_cell(family_name, column_qualifier, value)
  end

  defp nested_map(type, value, accum, family_name, column_qualifier) do
    if value == nil or value == "" do
      niled_map = nil_values(type)
      apply_mutations(type, niled_map, accum, family_name, column_qualifier)
    else
      apply_mutations(type, value, accum, family_name, column_qualifier)
    end
  end

  defp nil_values(type_spec) do
    Enum.reduce(type_spec, %{}, fn {k, v}, accum ->
      if is_map(v) do
        Map.put(accum, k, nil_values(v))
      else
        Map.put(accum, k, nil)
      end
    end)
  end

  defp column_qualifier(parent_key, key) do
    case parent_key do
      nil -> to_string(key)
      parent -> "#{parent}.#{to_string(key)}"
    end
  end
end
