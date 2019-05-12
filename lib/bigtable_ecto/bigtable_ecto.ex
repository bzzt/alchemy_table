defmodule Bigtable.Ecto do
  use Application

  def start(_, _) do
    children = [
      Bigtable.Ecto.Migration.Supervisor
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
