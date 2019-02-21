defmodule BT.Schema.RidePositionTest do
  use AlchemyTable.Schema

  type do
    column(:bearing, :integer)
    column(:latitude, :float)
    column(:longitude, :float)
    column(:timestamp, :string)
  end
end

defmodule BT.Schema.RideStateTest do
  use AlchemyTable.Schema

  table :ride_state, row_key: "RIDE#[ride.id]", ts: true do
    family :ride do
      column(:state, :string)
    end
  end
end

defmodule BT.Schema.RideTest do
  alias BT.Schema.{RidePositionTest, RideStateTest}
  use AlchemyTable.Schema

  @cloned [
    {:driver_ride, row_key: "RIDE#[ride.driver]#[ride.acceptedAt]"},
    {:ride_ts, ts: true}
  ]

  table :ride, row_key: "RIDE#[ride.id]" do
    family :ride do
      column(:acceptedAt, :string)
      column(:approachFrom, RidePositionTest)
      column(:driver, :string)
      column(:id, :string)
      promoted(:state, RideStateTest)
    end
  end
end
