defmodule Bigtable.Ecto.Migration.Supervisor do
  @moduledoc false

  use Supervisor

  alias Bigtable.Ecto.Migration.Runner

  def start_link(_) do
    IO.puts("HERE")
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    children = [
      Supervisor.child_spec(Runner,
        start: {Runner, :start_link, []}
      )
    ]

    Supervisor.init(children, strategy: :simple_one_for_one)
  end
end
