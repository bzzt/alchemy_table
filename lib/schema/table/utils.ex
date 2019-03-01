defmodule AlchemyTable.Table.Utils do
  alias Bigtable.MutateRow

  def atoms_from_dots(string) do
    string
    |> String.replace(~r/[\[\]]/, "")
    |> String.split(".")
    |> Enum.map(&String.to_atom/1)
  end

  def build_key_parts(key_pattern) do
    key_pattern
    |> String.split("#")
    |> Enum.map(fn string ->
      case Regex.run(~r/\[(.*)\]/, string) do
        [h | _] ->
          atoms_from_dots(h)

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
end
