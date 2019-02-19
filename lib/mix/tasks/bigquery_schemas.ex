defmodule Mix.Tasks.BigQuery.Schemas do
  use Mix.Task

  def run(_) do
    Path.wildcard(Path.join([Mix.Project.build_path(), "**/ebin/**/*.beam"]))
    |> Stream.map(&get_module_attributes/1)
    |> Stream.filter(&implements_schema?/1)
    |> Enum.map(&elem(&1, 0))
    |> Enum.map(&get_metadata/1)
    |> Enum.map(&translate_families/1)
    |> IO.inspect()
  end

  defp get_module_attributes(path) do
    {:ok, {mod, chunks}} = :beam_lib.chunks('#{path}', [:attributes])
    {mod, get_in(chunks, [:attributes, :behaviour])}
  end

  defp implements_schema?({_, behaviours}) do
    is_list(behaviours) and Enum.member?(behaviours, AlchemyTable.Schema)
  end

  defp get_metadata(module), do: apply(module, :metadata, [])

  defp translate_families(metadata) do
    base_spec = base_def(metadata)
    type_spec = Map.from_struct(metadata.type)

    column_defs =
      Enum.map(type_spec, fn {family, columns} ->
        %{
          familyId: Recase.CamelCase.convert(to_string(family)),
          onlyReadLatest: true,
          encoding: "TEXT",
          columns: translate_columns(columns)
        }
      end)

    bigtable_options = %{
      columnFamilies: column_defs,
      readRowkeyAsString: true
    }

    Map.put(base_spec, :bigtableOptions, bigtable_options)
  end

  defp translate_columns(columns, parent_key \\ []) do
    map_from_struct(columns)
    |> Enum.map(&translate_column(&1, parent_key))
    |> List.flatten()
  end

  defp translate_column({qualifier, type}, parent_key) when is_map(type) do
    translate_columns(type, [to_string(qualifier) | parent_key])
  end

  defp translate_column({qualifier, type}, parent_key) do
    qualifier = [to_string(qualifier) | parent_key] |> Enum.reverse()

    column_def = %{
      qualifier_string: qualifier |> Enum.join("."),
      type: translate_type(type)
    }

    if !Enum.empty?(parent_key) do
      [h | t] = qualifier

      field_name =
        [h | Enum.map(t, &Recase.to_pascal/1)]
        |> Enum.join()

      Map.put(column_def, :fieldName, field_name)
    else
      column_def
    end
  end

  defp base_def(%{instance: instance, name: name}) do
    %{
      sourceFormat: "BIGTABLE",
      sourceUris: [
        "https://googleapis.com/bigtable/#{instance}/tables/#{name}"
      ],
      nullMarker: "",
      bigtableOptions: %{
        readRowkeyAsString: true,
        columnFamilies: []
      }
    }
  end

  defp translate_type(:list), do: "STRING"
  defp translate_type(:map), do: "STRING"
  defp translate_type(type), do: String.upcase(to_string(type))

  defp map_from_struct(map) do
    if Map.has_key?(map, :__struct__) do
      Map.from_struct(map)
    else
      map
    end
  end
end
