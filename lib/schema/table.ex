defmodule AlchemyTable.Schema.Table do
  def atoms_from_dots(string) do
    string
    |> String.replace(~r/[\[\]]/, "")
    |> String.split(".")
    |> Enum.map(&String.to_atom/1)
  end

  def build_key_parts(opts) do
    Keyword.fetch!(opts, :row_key)
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

  def build_row_key(r, key_parts) do
    key_parts
    |> Enum.map(fn kp ->
      if is_list(kp) do
        get_in(r, kp)
      else
        kp
      end
    end)
    |> Enum.join("#")
  end

  #   def get_all do
  #     rows = Get.get_all(@prefix)

  #     rows
  #     |> parse_result()
  #   end

  #   def get_by_id(ids) when is_list(ids) do
  #     rows = Get.get_by_id(ids, @prefix)

  #     rows
  #     |> parse_result()
  #   end

  #   def get_by_id(id) when is_binary(id) do
  #     get_by_id([id])
  #   end

  #   def update(maps) when is_list(maps) do
  #     Update.update(__MODULE__.type(), maps, @prefix, @update_patterns)
  #   end

  # def update(map) when is_map(map) do
  # meta =
  # __MODULE__.metadata()
  # |> IO.inspect()
  # end

  #   def delete(ids) when is_list(ids) do
  #     Delete.delete_by_id(ids, @prefix)
  #   end

  #   def delete(id) when is_binary(id) do
  #     delete([id])
  #   end

  #   def delete_all do
  #     Delete.delete_all()
  #   end

  #   def parse_result(result) do
  #     Reads.parse_result(result, __MODULE__.type())
  #   end
end
