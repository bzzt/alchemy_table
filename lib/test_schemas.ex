# defmodule BT.Schema.PositionTest do
#   use Bigtable.Schema

#   @main_table %{
#     table: "ride-position",
#     row_prefix: "RIDE",
#     parent_key: "ride.id",
#     time_series: true
#   }

#   entity :ride_position do
#     family :position do
#       column(:address, :string)
#       column(:addressComponents, :map)
#       column(:latitude, :float)
#       column(:longitude, :float)
#     end
#   end
# end

# defmodule BT.Schema.StateTest do
#   use Bigtable.Schema

#   @main_table %{
#     table: "ride-state",
#     row_prefix: "RIDE",
#     parent_key: "ride.id"
#   }

#   entity :ride_state do
#     family :state do
#       column(:state, :string)
#     end
#   end
# end

# defmodule BT.Schema.RideTest do
#   @moduledoc false
#   use Bigtable.Schema

#   @main_table %{
#     table: "ride",
#     row_prefix: "RIDE",
#     key: "ride.id"
#   }

#   @extra_tables [
#     %{
#       table: "driver-ride",
#       row_prefix: "RIDE",
#       key: "ride.driver",
#       time_series: true
#     }
#   ]

#   entity :ride do
#     family :ride do
#       column(:driver, :string)
#       column(:id, :string)
#       column(:firstRide, :boolean)
#       column(:position, BT.Schema.PositionTest)
#       column(:state, :string, BT.Schema.StateTest)
#     end
#   end
# end
