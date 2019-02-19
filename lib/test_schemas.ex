# defmodule BT.Schema.PositionTest do
#   use AlchemyTable.Schema

#   row :ride_position do
#     family :position do
#       column(:address, :string)
#       column(:addressComponents, :map)
#       column(:latitude, :float)
#       column(:longitude, :float)
#     end
#   end
# end

# defmodule BT.Schema.StateTest do
#   use AlchemyTable.Schema

#   row :ride_state do
#     family :state do
#       column(:state, :string)
#     end
#   end
# end

# defmodule BT.Schema.RideTest do
#   use AlchemyTable.Schema

#   row :ride do
#     family :ride do
#       column(:driver, :string)
#       column(:id, :string)
#       column(:firstRide, :boolean)
#       column(:position, BT.Schema.PositionTest)
#       column(:state, BT.Schema.StateTest)
#     end
#   end
# end
