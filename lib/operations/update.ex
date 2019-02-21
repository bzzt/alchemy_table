defmodule AlchemyTable.Operations.Update do
  @moduledoc false
  alias AlchemyTable.{Mutations, Utils, Validation}
  # alias Bigtable.MutateRows

  # def update(type_spec, maps, row_prefix, update_patterns) do
  #   mutations = mutations_from_maps(type_spec, maps, row_prefix, update_patterns)

  #   mutations
  #   |> MutateRows.mutate()
  # end

  # @spec mutations_from_maps(map(), [map()], binary(), [binary()]) ::
  #         Google.Bigtable.V2.MutateRows.Entry.t()
  # def mutations_from_maps(type_spec, maps, row_prefix, update_patterns) do
  #   Enum.each(maps, &Validation.validate_map!(type_spec, &1))

  #   mutations = Enum.map(maps, &mutations_from_map(type_spec, &1, row_prefix, update_patterns))

  #   mutations
  #   |> List.flatten()
  #   |> MutateRows.build()
  # end

  def update(row_key, type_spec, map) do
    Mutations.create_mutations(row_key, type_spec, map)
  end
end
