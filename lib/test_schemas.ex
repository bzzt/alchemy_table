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

  table :ride, row_key: "RIDE#[ride.id]" do
    cloned(:driver_ride, row_key: "RIDE#[ride.driver]#[ride.acceptedAt]")
    cloned(:ride_ts, ts: true)

    family :ride do
      column(:acceptedAt, :string)
      column(:approachFrom, RidePositionTest)
      column(:driver, :string)
      column(:id, :string)
      promoted(:state, RideStateTest)
      promoted(:other, RideStateTest)
    end

    family :other do
      column(:a, :string)
    end
  end
end
