defmodule Mix.Tasks.BigQuery.Schemas do
  use Mix.Task
  @schema_dir Path.join([File.cwd!(), "/schema-defs"])
  def run(_) do
    File.mkdir(@schema_dir)

    Path.wildcard(Path.join([Mix.Project.build_path(), "**/ebin/**/*.beam"]))
    |> Enum.map(&get_module_attributes/1)
    |> Enum.filter(&implements_schema?/1)
    |> Enum.map(&elem(&1, 0))
    |> Enum.map(&get_metadata/1)
    |> Enum.map(&build_definition/1)
    |> Enum.map(&clone_definition/1)
    |> List.flatten()
    |> Enum.each(&write_def/1)
  end

  defp write_def({metadata, def_header, def_body}) do
    definition =
      Map.merge(def_header, def_body)
      |> Poison.encode!(pretty: true)

    "#{@schema_dir}/#{metadata.name}.json"
    |> File.write!(definition)
  end

  defp get_module_attributes(path) do
    {:ok, {mod, chunks}} = :beam_lib.chunks('#{path}', [:attributes])
    {mod, get_in(chunks, [:attributes, :behaviour])}
  end

  defp implements_schema?({_, behaviours}) do
    is_list(behaviours) and Enum.member?(behaviours, AlchemyTable.Schema)
  end

  defp get_metadata(module), do: apply(module, :metadata, [])

  defp build_definition(metadata) do
    header = def_header(metadata)
    type_spec = Map.from_struct(metadata.schema)

    definition = %{
      bigtableOptions: %{
        columnFamilies: translate_column_families(type_spec),
        readRowkeyAsString: true
      }
    }

    {metadata, header, definition}
  end

  defp clone_definition({metadata, _header, body} = definition) do
    if metadata.cloned == nil do
      definition
    else
      cloned =
        metadata.cloned
        |> Enum.map(fn to_clone ->
          meta = %{metadata | name: to_clone}
          header = def_header(meta)
          {meta, header, body}
        end)

      [definition | cloned]
    end
  end

  defp translate_column_families(type_spec) do
    Enum.map(type_spec, fn {family, columns} ->
      %{
        familyId: Recase.CamelCase.convert(to_string(family)),
        onlyReadLatest: true,
        encoding: "TEXT",
        columns: translate_columns(columns)
      }
    end)
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
      qualifierString: qualifier |> Enum.join("."),
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

  defp def_header(%{instance: instance, name: name}) do
    table_name =
      to_string(name)
      |> Recase.KebabCase.convert()

    %{
      sourceFormat: "BIGTABLE",
      sourceUris: [
        "https://googleapis.com/bigtable/#{instance}/tables/#{table_name}"
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
