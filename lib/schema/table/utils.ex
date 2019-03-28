defmodule AlchemyTable.Table.Utils do
  @moduledoc false

  def atoms_from_dots(string) do
    string
    |> String.split(".")
    |> Enum.map(&String.to_atom/1)
  end

  def build_key_parts(key_pattern) do
    key_pattern
    |> String.split("#")
    |> Enum.map(fn string ->
      case Regex.run(~r/\[(.*)\]/, string) do
        [h | _] ->
          h
          |> remove_brackets()
          |> atoms_from_dots()

        nil ->
          string
      end
    end)
  end

  def build_row_key(key_parts, data) do
    key_parts
    |> Enum.map(fn kp ->
      if is_list(kp) do
        get_in(data, kp)
      else
        kp
      end
    end)
    |> Enum.join("#")
  end

  def get_key_pattern!(opts) do
    Keyword.fetch!(opts, :row_key)
  end

  def get_key_pattern(opts) do
    Keyword.get(opts, :row_key)
  end

  def full_name(instance, table_name) do
    base_name = table_name |> to_string() |> Recase.to_kebab()

    table_name =
      case Application.get_env(:alchemy_table, :table_prefix, nil) do
        nil ->
          base_name

        prefix ->
          "#{prefix}-#{base_name}"
      end

    "#{instance}/tables/#{table_name}"
  end

  defp remove_brackets(string) do
    string
    |> String.replace(~r/[\[\]]/, "")
  end
end
