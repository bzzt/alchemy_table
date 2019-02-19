# defmodule AlchemyTable.Schema.Table do
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

#   def update(map) when is_map(map) do
#     update([map])
#   end

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
# end
