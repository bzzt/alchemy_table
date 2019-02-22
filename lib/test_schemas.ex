defmodule BT.Schema.RidePositionTest do
  use AlchemyTable.Type

  type do
    field(:bearing, :integer)
    field(:latitude, :float)
    field(:longitude, :float)
    field(:timestamp, :string)
  end
end

defmodule BT.Schema.RideStateTest do
  use AlchemyTable.Table

  table :ride_state, row_key: "RIDE#[ride.id]", ts: true do
    family :ride do
      column(:state, :string)
    end
  end
end

defmodule BT.Schema.RideTest do
  alias BT.Schema.{DriverRideTest, RidePositionTest, RideStateTest, RideTsTest}
  use AlchemyTable.Table

  @cloned [
    RideTsTest,
    DriverRideTest
  ]

  table :ride, row_key: "RIDE#[ride.id]" do
    family :ride do
      column(:acceptedAt, :string)
      column(:approachFrom, RidePositionTest)
      column(:driver, :string)
      column(:id, :string)
      promoted(:state, RideStateTest)
    end

    family :other do
      column(:a, :string)
    end
  end
end

defmodule BT.Schema.RideTsTest do
  alias BT.Schema.RideTest
  use AlchemyTable.Table

  table :ride_ts, row_key: "RIDE#[ride.id]", ts: true do
    clone(RideTest)
  end
end

defmodule BT.Schema.DriverRideTest do
  alias BT.Schema.RideTest
  use AlchemyTable.Table

  table :driver_ride, row_key: "RIDE#[ride.driver]#[ride.acceptedAt]" do
    clone(RideTest)
  end
end
