defmodule Bigtable.Ecto.Migration do
  alias Google.Bigtable.Admin.V2.GcRule

  defmodule Index do
    @moduledoc """
    Used internally by adapters.

    To define an index in a migration, see `Ecto.Migration.index/3`.
    """
    defstruct table: nil,
              prefix: nil,
              name: nil,
              columns: [],
              unique: false,
              concurrently: false,
              using: nil,
              where: nil,
              comment: nil,
              options: nil

    @type t :: %__MODULE__{
            table: String.t(),
            prefix: atom,
            name: atom,
            columns: [atom | String.t()],
            unique: boolean,
            concurrently: boolean,
            using: atom | String.t(),
            where: atom | String.t(),
            comment: String.t() | nil,
            options: String.t()
          }
  end

  defmodule Table do
    @moduledoc """
    Used internally by adapters.

    To define a table in a migration, see `Ecto.Migration.table/2`.
    """
    defstruct name: nil, prefix: nil, comment: nil, primary_key: true, engine: nil, options: nil

    @type t :: %__MODULE__{
            name: String.t(),
            prefix: atom | nil,
            comment: String.t() | nil,
            primary_key: boolean,
            engine: atom,
            options: String.t()
          }
  end

  defmodule Reference do
    @moduledoc """
    Used internally by adapters.

    To define a reference in a migration, see `Ecto.Migration.references/2`.
    """
    defstruct name: nil,
              table: nil,
              column: :id,
              type: :bigserial,
              on_delete: :nothing,
              on_update: :nothing

    @type t :: %__MODULE__{
            table: String.t(),
            column: atom,
            type: atom,
            on_delete: atom,
            on_update: atom
          }
  end

  defmodule Constraint do
    @moduledoc """
    Used internally by adapters.

    To define a constraint in a migration, see `Ecto.Migration.constraint/3`.
    """
    defstruct name: nil, table: nil, check: nil, exclude: nil, prefix: nil, comment: nil

    @type t :: %__MODULE__{
            name: atom,
            table: String.t(),
            prefix: atom | nil,
            check: String.t() | nil,
            exclude: String.t() | nil,
            comment: String.t() | nil
          }
  end

  defmodule Command do
    @moduledoc """
    Used internally by adapters.

    This represents the up and down legs of a reversible raw command
    that is usually defined with `Ecto.Migration.execute/1`.

    To define a reversible command in a migration, see `Ecto.Migration.execute/2`.
    """
    defstruct up: nil, down: nil
    @type t :: %__MODULE__{up: String.t(), down: String.t()}
  end

  alias Bigtable.Ecto.Migration.Runner

  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      import Bigtable.Ecto.Migration
      @disable_ddl_transaction false
      @before_compile Bigtable.Ecto.Migration
    end
  end

  @doc false
  defmacro __before_compile__(_env) do
    quote do
      def __migration__,
        do: [disable_ddl_transaction: @disable_ddl_transaction]
    end
  end

  @doc """
  Creates a table.

  By default, the table will also include an `:id` primary key field that
  has a type of `:bigserial`. Check the `table/2` docs for more information.

  ## Examples

      create table(:posts) do
        add :title, :string, default: "Untitled"
        add :body,  :text

        timestamps()
      end

  """
  defmacro create(object, do: block) do
    expand_create(object, :create, block)
  end

  @doc """
  Creates a table if it does not exist.

  Works just like `create/2` but does not raise an error when the table
  already exists.
  """
  defmacro create_if_not_exists(object, do: block) do
    expand_create(object, :create_if_not_exists, block)
  end

  defp expand_create(object, command, block) do
    quote do
      table = %Table{} = unquote(object)
      Runner.start_command({unquote(command), Bigtable.Ecto.Migration.__prefix__(table)})

      unquote(block)
      Runner.end_command()
      table
    end
  end

  @doc """
  Alters a table.

  ## Examples

      alter table("posts") do
        add :summary, :text
        modify :title, :text
        remove :views
      end

  """
  defmacro alter(object, do: block) do
    quote do
      table = %Table{} = unquote(object)
      Runner.start_command({:alter, Bigtable.Ecto.Migration.__prefix__(table)})
      unquote(block)
      Runner.end_command()
    end
  end

  @doc """
  Creates one of the following:

    * an index
    * a table with only an `:id` field
    * a constraint

  When reversing (in a `change/0` running backwards), indexes are only dropped
  if they exist, and no errors are raised. To enforce dropping an index, use
  `drop/1`.

  ## Examples

      create index("posts", [:name])
      create table("version")
      create constraint("products", "price_must_be_positive", check: "price > 0")

  """
  def create(%Index{} = index) do
    Runner.execute({:create, __prefix__(index)})
    index
  end

  def create(%Constraint{} = constraint) do
    Runner.execute({:create, __prefix__(constraint)})
    constraint
  end

  def create(%Table{} = table) do
    do_create(table, :create)
    table
  end

  @doc """
  Creates an index or a table with only `:id` field if one does not yet exist.

  ## Examples

      create_if_not_exists index("posts", [:name])

      create_if_not_exists table("version")

  """
  def create_if_not_exists(%Index{} = index) do
    Runner.execute({:create_if_not_exists, __prefix__(index)})
  end

  def create_if_not_exists(%Table{} = table) do
    do_create(table, :create_if_not_exists)
  end

  defp do_create(table, command) do
    columns =
      if table.primary_key do
        [{:add, :id, :bigserial, primary_key: true}]
      else
        []
      end

    Runner.execute({command, __prefix__(table), columns})
  end

  @doc """
  Drops one of the following:

    * an index
    * a table
    * a constraint

  ## Examples

      drop index("posts", [:name])
      drop table("posts")
      drop constraint("products", "price_must_be_positive")

  """
  def drop(%{} = index_or_table_or_constraint) do
    Runner.execute({:drop, __prefix__(index_or_table_or_constraint)})
    index_or_table_or_constraint
  end

  @doc """
  Drops a table or index if it exists.

  Does not raise an error if the specified table or index does not exist.

  ## Examples

      drop_if_exists index("posts", [:name])
      drop_if_exists table("posts")

  """
  def drop_if_exists(%{} = index_or_table) do
    Runner.execute({:drop_if_exists, __prefix__(index_or_table)})
    index_or_table
  end

  @doc """
  Returns a table struct that can be given to `create/2`, `alter/2`, `drop/1`,
  etc.

  ## Examples

      create table("products") do
        add :name, :string
        add :price, :decimal
      end

      drop table("products")

      create table("products", primary_key: false) do
        add :name, :string
        add :price, :decimal
      end

  ## Options

    * `:primary_key` - when `false`, a primary key field is not generated on table
      creation.
    * `:engine` - customizes the table storage for supported databases. For MySQL,
      the default is InnoDB.
    * `:prefix` - the prefix for the table.
    * `:options` - provide custom options that will be appended after the generated
      statement. For example, "WITH", "INHERITS", or "ON COMMIT" clauses.

  """
  def table(name, opts \\ [])

  def table(name, opts) when is_atom(name) do
    table(Atom.to_string(name), opts)
  end

  def table(name, opts) when is_binary(name) and is_list(opts) do
    struct(%Table{name: name}, opts)
  end

  @doc ~S"""
  Returns an index struct that can be given to `create/1`, `drop/1`, etc.

  Expects the table name as the first argument and the index field(s) as
  the second. The fields can be atoms, representing columns, or strings,
  representing expressions that are sent as-is to the database.

  ## Options

    * `:name` - the name of the index. Defaults to "#{table}_#{column}_index".
    * `:unique` - indicates whether the index should be unique. Defaults to
      `false`.
    * `:concurrently` - indicates whether the index should be created/dropped
      concurrently.
    * `:using` - configures the index type.
    * `:prefix` - specify an optional prefix for the index.
    * `:where` - specify conditions for a partial index.

  ## Adding/dropping indexes concurrently

  PostgreSQL supports adding/dropping indexes concurrently (see the
  [docs](http://www.postgresql.org/docs/9.4/static/sql-createindex.html)).
  In order to take advantage of this, the `:concurrently` option needs to be set
  to `true` when the index is created/dropped.

  **Note**: in order for the `:concurrently` option to work, the migration must
  not be run inside a transaction. See the `Ecto.Migration` docs for more
  information on running migrations outside of a transaction.

  ## Index types

  When creating an index, the index type can be specified with the `:using`
  option. The `:using` option can be an atom or a string, and its value is
  passed to the generated `USING` clause as-is.

  For example, PostgreSQL supports several index types like B-tree (the
  default), Hash, GIN, and GiST. More information on index types can be found
  in the [PostgreSQL docs]
  (http://www.postgresql.org/docs/9.4/static/indexes-types.html).

  ## Partial indexes

  Databases like PostgreSQL and MSSQL support partial indexes.

  A partial index is an index built over a subset of a table. The subset
  is defined by a conditional expression using the `:where` option.
  The `:where` option can be an atom or a string; its value is passed
  to the generated `WHERE` clause as-is.

  More information on partial indexes can be found in the [PostgreSQL
  docs](http://www.postgresql.org/docs/9.4/static/indexes-partial.html).

  ## Examples

      # With no name provided, the name of the below index defaults to
      # products_category_id_sku_index
      create index("products", [:category_id, :sku], unique: true)

      # The name can also be set explicitly
      drop index("products", [:category_id, :sku], name: :my_special_name)

      # Indexes can be added concurrently
      create index("products", [:category_id, :sku], concurrently: true)

      # The index type can be specified
      create index("products", [:name], using: :hash)

      # Partial indexes are created by specifying a :where option
      create index("products", [:user_id], where: "price = 0", name: :free_products_index)

  Indexes also support custom expressions. Some databases may require the
  index expression to be written between parentheses:

      # Create an index on a custom expression
      create index("products", ["(lower(name))"], name: :products_lower_name_index)

      # Create a tsvector GIN index on PostgreSQL
      create index("products", ["(to_tsvector('english', name))"],
                   name: :products_name_vector, using: "GIN")
  """
  def index(table, columns, opts \\ [])

  def index(table, columns, opts) when is_atom(table) do
    index(Atom.to_string(table), columns, opts)
  end

  def index(table, column, opts) when is_binary(table) and is_atom(column) do
    index(table, [column], opts)
  end

  def index(table, columns, opts) when is_binary(table) and is_list(columns) and is_list(opts) do
    validate_index_opts!(opts)
    index = struct(%Index{table: table, columns: columns}, opts)
    %{index | name: index.name || default_index_name(index)}
  end

  @doc """
  Shortcut for creating a unique index.

  See `index/3` for more information.
  """
  def unique_index(table, columns, opts \\ [])

  def unique_index(table, columns, opts) when is_list(opts) do
    index(table, columns, [unique: true] ++ opts)
  end

  defp default_index_name(index) do
    [index.table, index.columns, "index"]
    |> List.flatten()
    |> Enum.map(&to_string(&1))
    |> Enum.map(&String.replace(&1, ~r"[^\w_]", "_"))
    |> Enum.map(&String.replace_trailing(&1, "_", ""))
    |> Enum.join("_")
    |> String.to_atom()
  end

  @doc """
  Executes arbitrary SQL or a keyword command.

  Reversible commands can be defined by calling `execute/2`.

  ## Examples

      execute "CREATE EXTENSION postgres_fdw"

      execute create: "posts", capped: true, size: 1024

  """
  def execute(command) when is_binary(command) or is_list(command) do
    Runner.execute(command)
  end

  @doc """
  Executes reversible SQL commands.

  This is useful for database-specific functionality that does not
  warrant special support in Ecto, for example, creating and dropping
  a PostgreSQL extension. The `execute/2` form avoids having to define
  separate `up/0` and `down/0` blocks that each contain an `execute/1`
  expression.

  ## Examples

      execute "CREATE EXTENSION postgres_fdw", "DROP EXTENSION postgres_fdw"

  """
  def execute(up, down)
      when (is_binary(up) or is_list(up)) and
             (is_binary(down) or is_list(down)) do
    Runner.execute(%Command{up: up, down: down})
  end

  @doc """
  Gets the migrator direction.
  """
  @spec direction :: :up | :down
  def direction do
    Runner.migrator_direction()
  end

  @doc """
  Gets the migrator prefix.
  """
  def prefix do
    Runner.prefix()
  end

  def add(family, gc_rule, opts \\ [])

  def add(family, %GcRule{} = gc_rule, opts)
      when is_atom(family) and is_list(opts) do
    Runner.subcommand({:add, family, gc_rule, opts})
  end

  @doc """
  Renames a table.

  ## Examples

      rename table("posts"), to: table("new_posts")
  """
  def rename(%Table{} = table_current, to: %Table{} = table_new) do
    Runner.execute({:rename, __prefix__(table_current), __prefix__(table_new)})
    table_new
  end

  @doc """
  Renames a column outside of the `alter` statement.

  ## Examples

      rename table("posts"), :title, to: :summary
  """
  def rename(%Table{} = table, current_column, to: new_column)
      when is_atom(current_column) and is_atom(new_column) do
    Runner.execute({:rename, __prefix__(table), current_column, new_column})
    table
  end

  @doc """
  Generates a fragment to be used as a default value.

  ## Examples

      create table("posts") do
        add :inserted_at, :naive_datetime, default: fragment("now()")
      end
  """
  def fragment(expr) when is_binary(expr) do
    {:fragment, expr}
  end

  @doc """
  Modifies the type of a column when altering a table.

  This command is not reversible unless the `:from` option is provided.
  If the `:from` value is a `%Reference{}`, the adapter will try to drop
  the corresponding foreign key constraints before modifying the type.

  See `add/3` for more information on supported types.

  ## Examples

      alter table("posts") do
        modify :title, :text
      end
  """
  def modify(family, gc_rule, opts \\ [])

  def modify(family, %GcRule{} = gc_rule, opts) when is_atom(family) and is_list(opts) do
    Runner.subcommand({:modify, family, gc_rule, opts})
  end

  @doc """
  Removes a column when altering a table.

  This command is not reversible as Ecto does not know what type it should add
  the column back as. See `remove/3` as a reversible alternative.

  ## Examples

      alter table("posts") do
        remove :title
      end

  """
  def remove(family) when is_atom(family) do
    Runner.subcommand({:remove, family})
  end

  @doc """
  Executes queue migration commands.

  Reverses the order in which commands are executed when doing a rollback
  on a `change/0` function and resets the commands queue.
  """
  def flush do
    Runner.flush()
  end

  defp validate_index_opts!(opts) when is_list(opts) do
    case Keyword.get_values(opts, :where) do
      [_, _ | _] ->
        raise ArgumentError,
              "only one `where` keyword is supported when declaring a partial index. " <>
                "To specify multiple conditions, write a single WHERE clause using AND between them"

      _ ->
        :ok
    end
  end

  defp validate_index_opts!(opts), do: opts

  @doc false
  def __prefix__(%{prefix: prefix} = index_or_table) do
    runner_prefix = Runner.prefix()

    cond do
      is_nil(prefix) ->
        prefix = runner_prefix || Runner.repo_config(:migration_default_prefix, nil)
        %{index_or_table | prefix: prefix}

      is_nil(runner_prefix) or runner_prefix == to_string(prefix) ->
        index_or_table

      true ->
        raise Ecto.MigrationError,
          message:
            "the :prefix option `#{prefix}` does match the migrator prefix `#{runner_prefix}`"
    end
  end
end
