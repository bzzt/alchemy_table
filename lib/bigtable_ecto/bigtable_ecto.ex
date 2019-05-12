defmodule Bigtable.Ecto do
  use Application

  def start(_, _) do
    IO.puts("INSIDE APP")

    children = [
      Bigtable.Ecto.Migration.Supervisor
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
