# defmodule BT.Schema.VehicleStateTest do
#   use AlchemyTable.Schema

#   table :vehicle_state do
#     family :vehicle do
#       column(:state, :string)
#     end
#   end
# end

defmodule BT.Schema.VehiclePositionTest do
  use AlchemyTable.Schema

  type do
    column(:bearing, :integer)
    column(:latitude, :float)
    column(:longitude, :float)
    column(:timestamp, :string)
  end
end

# defmodule BT.Schema.VehicleTest do
#   alias BT.Schema.{VehiclePositionTest, VehicleStateTest}
#   use AlchemyTable.Schema

#   table :vehicle do
#     family :vehicle do
#       column(:battery, :integer)
#       column(:checkedInAt, :string)
#       column(:condition, :string)
#       column(:driver, :string)
#       column(:fleet, :string)
#       column(:id, :string)
#       column(:numberPlate, :string)
#       column(:position, VehiclePositionTest)
#       column(:previousPosition, VehiclePositionTest)
#       column(:ride, :string)
#       promoted(:state, VehicleStateTest)
#     end
#   end
# end

defmodule BT.Schema.RideTest do
  alias BT.Schema.{VehiclePositionTest}
  use AlchemyTable.Schema

  @cloned [
    :user_ride,
    :driver_ride,
    {:ride_ts, ts: true}
  ]

  table :ride, row_key: "RIDE#[ride.id]" do
    family :ride do
      column(:acceptedAt, :string)
      column(:approachFrom, VehiclePositionTest)
    end
  end
end
