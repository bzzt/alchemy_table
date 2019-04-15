defmodule AlchemyTable.BigQuery do
  @moduledoc """
  Provides BigQuery schema definition file generation for AlchemyTable defined tables
  """
  def generate_schema(metadata) do
    metadata
    |> build_definition()
    |> write_def()
  end

  defp write_def({metadata, def_header, def_body}) do
    def_path = Path.join([File.cwd!(), "schema-defs"])
    env_path = Path.join([def_path, to_string(Mix.env())])
    definition = to_json(def_header, def_body)

    File.mkdir(def_path)
    File.mkdir(env_path)

    "#{env_path}/#{metadata.name}.json"
    |> File.write!(definition, [:write])
  end

  defp to_json(header, body) do
    schema_def = Map.merge(header, body)

    schema_def
    |> Poison.encode!(pretty: true)
  end

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
    map = map_from_struct(columns)

    map
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

    if Enum.empty?(parent_key) do
      column_def
    else
      [h | t] = qualifier

      field_name =
        [h | Enum.map(t, &Recase.to_pascal/1)]
        |> Enum.join()

      Map.put(column_def, :fieldName, field_name)
    end
  end

  defp def_header(%{instance: instance, table_name: table_name}) do
    full_name = AlchemyTable.Table.Utils.full_name(instance, table_name)

    %{
      sourceFormat: "BIGTABLE",
      sourceUris: [
        "https://googleapis.com/bigtable/#{full_name}"
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
