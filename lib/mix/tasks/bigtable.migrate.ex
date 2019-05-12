defmodule Mix.Tasks.Bigtable.Migrate do
  use Mix.Task
  import Mix.Ecto
  import Mix.Bigtable.Ecto

  @shortdoc "Runs the repository migrations"

  @aliases [
    n: :step,
    r: :repo
  ]

  @switches [
    all: :boolean,
    step: :integer,
    to: :integer,
    quiet: :boolean,
    prefix: :string,
    pool_size: :integer,
    log_sql: :boolean,
    strict_version_order: :boolean,
    repo: [:keep, :string],
    no_compile: :boolean,
    no_deps_check: :boolean
  ]
  @moduledoc """
  Runs the pending migrations for the given repository.

  Migrations are expected at "priv/YOUR_REPO/migrations" directory
  of the current application, where "YOUR_REPO" is the last segment
  in your repository name. For example, the repository `MyApp.Repo`
  will use "priv/repo/migrations". The repository `Whatever.MyRepo`
  will use "priv/my_repo/migrations".
  You can configure a repository to use another directory by specifying
  the `:priv` key under the repository configuration. The "migrations"
  part will be automatically appended to it. For instance, to use
  "priv/custom_repo/migrations":

      config :my_app, MyApp.Repo, priv: "priv/custom_repo"

  This task runs all pending migrations by default. To migrate up to a
  specific version number, supply `--to version_number`. To migrate a
  specific number of times, use `--step n`.

  The repositories to migrate are the ones specified under the
  `:ecto_repos` option in the current app configuration. However,
  if the `-r` option is given, it replaces the `:ecto_repos` config.

  Since Ecto tasks can only be executed once, if you need to migrate
  multiple repositories, set `:ecto_repos` accordingly or pass the `-r`
  flag multiple times.

  If a repository has not yet been started, one will be started outside
  your application supervision tree and shutdown afterwards.

  ## Examples

      mix ecto.migrate
      mix ecto.migrate -r Custom.Repo

      mix ecto.migrate -n 3
      mix ecto.migrate --step 3

      mix ecto.migrate -v 20080906120000
      mix ecto.migrate --to 20080906120000

  ## Command line options

    * `-r`, `--repo` - the repo to migrate
    * `--all` - run all pending migrations
    * `--step` / `-n` - run n number of pending migrations
    * `--to` - run all migrations up to and including version
    * `--quiet` - do not log migration commands
    * `--prefix` - the prefix to run migrations on
    * `--pool-size` - the pool size if the repository is started only for the task (defaults to 1)
    * `--log-sql` - log the raw sql migrations are running
    * `--strict-version-order` - abort when applying a migration with old timestamp
    * `--no-compile` - does not compile applications before migrating
    * `--no-deps-check` - does not check depedendencies before migrating

  """

  @impl true
  def run(args, migrator \\ &Bigtable.Ecto.Migrator.run/4) do
    repos = parse_repo(args)
    {opts, _} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)

    opts =
      if opts[:to] || opts[:step] || opts[:all],
        do: opts,
        else: Keyword.put(opts, :all, true)

    opts =
      if opts[:quiet],
        do: Keyword.merge(opts, log: false, log_sql: false),
        else: opts

    # Start ecto_sql explicitly before as we don't need
    # to restart those apps if migrated.
    {:ok, foo} = Application.ensure_all_started(:bigtable_ecto)

    IO.inspect(foo)

    for repo <- repos do
      ensure_repo(repo, args)
      path = ensure_migrations_path(repo)
      pool = repo.config[:pool]

      fun =
        if function_exported?(pool, :unboxed_run, 2) do
          &pool.unboxed_run(&1, fn -> migrator.(&1, path, :up, opts) end)
        else
          &migrator.(&1, path, :up, opts)
        end

      case Bigtable.Ecto.Migrator.with_repo(repo, fun, [mode: :temporary] ++ opts) do
        {:ok, migrated, apps} ->
          restart_apps_if_migrated(apps, migrated)

        {:error, error} ->
          Mix.raise("Could not start repo #{inspect(repo)}, error: #{inspect(error)}")
      end
    end

    :ok
  end

  defp ensure_migrations_path(repo) do
    path = Path.join(source_repo_priv(repo), "migrations")

    if not Mix.Project.umbrella?() and not File.dir?(path) do
      raise_missing_migrations(Path.relative_to_cwd(path), repo)
    end

    path
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

  def ensure_started(repo, opts) do
    {:ok, started} = Application.ensure_all_started(:"bigtable.ecto")

    # If we starting EctoSQL just now, assume
    # logger has not been properly booted yet.
    if :bigtable_ecto in started && Process.whereis(Logger) do
      backends = Application.get_env(:logger, :backends, [])

      try do
        Logger.App.stop()
        Application.put_env(:logger, :backends, [:console])
        :ok = Logger.App.start()
      after
        Application.put_env(:logger, :backends, backends)
      end
    end

    {:ok, apps} = repo.__adapter__.ensure_all_started(repo.config(), :temporary)
    pool_size = Keyword.get(opts, :pool_size, 2)

    case repo.start_link(pool_size: pool_size) do
      {:ok, pid} ->
        {:ok, pid, apps}

      {:error, {:already_started, _pid}} ->
        {:ok, nil, apps}

      {:error, error} ->
        Mix.raise("Could not start repo #{inspect(repo)}, error: #{inspect(error)}")
    end
  end

  defp restart_apps_if_migrated(_apps, []), do: :ok

  defp restart_apps_if_migrated(apps, [_ | _]) do
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
end
