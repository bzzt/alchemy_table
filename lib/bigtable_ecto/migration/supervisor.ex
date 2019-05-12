defmodule Bigtable.Ecto.Migration.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    children = [
      Supervisor.child_spec(Bigtable.Ecto.Migration.Runner,
        start: {Bigtable.Ecto.Migration.Runner, :start_link, []}
      )
    ]

    Supervisor.init(children, strategy: :simple_one_for_one)
  end
end
