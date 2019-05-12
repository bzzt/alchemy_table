defmodule Mix.Bigtable.Ecto do
  def ensure_migrations_path(repo) do
    path = Path.join(source_repo_priv(repo), "migrations")

    if not Mix.Project.umbrella?() and not File.dir?(path) do
      raise_missing_migrations(Path.relative_to_cwd(path), repo)
    end

    path
  end

  def source_repo_priv(repo) do
    config = repo.config()
    priv = config[:priv] || "priv/#{repo |> Module.split() |> List.last() |> Macro.underscore()}"
    app = Keyword.fetch!(config, :otp_app)
    Path.join(Mix.Project.deps_paths()[app] || File.cwd!(), priv)
  end

  def migration_module do
    case Application.get_env(:bigtable_ecto, :migration_module, Bigtable.Ecto.Migration) do
      migration_module when is_atom(migration_module) -> migration_module
      other -> Mix.raise("Expected :migration_module to be a module, got: #{inspect(other)}")
    end
  end

  def restart_apps_if_migrated(_apps, []), do: :ok

  def restart_apps_if_migrated(apps, [_ | _]) do
    # Silence the logger to avoid application down messages.
    Logger.remove_backend(:console)

    for app <- Enum.reverse(apps) do
      Application.stop(app)
    end

    for app <- apps do
      Application.ensure_all_started(app)
    end

    :ok
  after
    Logger.add_backend(:console, flush: true)
  end

  defp raise_missing_migrations(path, repo) do
    Mix.raise("""
    Could not find migrations directory #{inspect(path)}
    for repo #{inspect(repo)}.
    This may be because you are in a new project and the
    migration directory has not been created yet. Creating an
    empty directory at the path above will fix this error.
    If you expected existing migrations to be found, please
    make sure your repository has been properly configured
    and the configured path exists.
    """)
  end
end
