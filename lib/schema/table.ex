defmodule AlchemyTable.Table do
  alias AlchemyTable.BigQuery

  defmacro __using__(_opt) do
    quote do
      import unquote(__MODULE__)
      import AlchemyTable.Table.Utils
      Module.register_attribute(__MODULE__, :families, accumulate: true)
      Module.register_attribute(__MODULE__, :promoted, accumulate: true)
      Module.register_attribute(__MODULE__, :cloned, accumulate: true)

      @after_compile unquote(__MODULE__)
    end
  end

  def __after_compile__(env, _) do
    should_gen =
      Application.get_env(:alchemy_table, :bigquery, [])
      |> Keyword.get(:gen_schemas, false)

    if should_gen do
      env.module
      |> apply(:__alchemy_metadata__, [])
      |> BigQuery.generate_schema()
    end
  end

  defmacro table(name, opts, do: block) do
    instance = Bigtable.Utils.configured_instance_name()

    quote do
      alias AlchemyTable.Operations.{Get, Update}
      alias AlchemyTable.Table
      alias Bigtable.ReadRows
      alias Google.Bigtable.V2.{ReadRowsRequest, RowFilter}
      @key_parts unquote(opts) |> get_key_pattern!() |> build_key_parts()
      unquote(block)
      defstruct @families

      def __alchemy_metadata__ do
        %{
          name: unquote(name),
          instance: unquote(instance),
          cloned: @cloned |> List.flatten(),
          key_parts: @key_parts,
          promoted: @promoted,
          opts: unquote(opts),
          table_name: unquote(name),
          schema: __alchemy_schema__(),
          full_name: Table.Utils.full_name(unquote(instance), unquote(name))
        }
      end

      def __alchemy_schema__ do
        %__MODULE__{}
      end

      def update(data, opts \\ [timestamp: DateTime.utc_now()]) do
        Update.update(__MODULE__, data, opts)
      end

      def get(request \\ ReadRows.build())

      def get(%RowFilter{} = filter) do
        request =
          ReadRows.build()
          |> Map.put(:filter, filter)

        Get.get(__alchemy_metadata__(), request)
      end

      def get(%ReadRowsRequest{} = request) do
        Get.get(__alchemy_metadata__(), request)
      end
    end
  end

  defmacro clone(module) do
    families =
      Macro.expand(module, __CALLER__)
      |> apply(:__alchemy_schema__, [])
      |> Map.from_struct()
      |> Map.to_list()
      |> Macro.escape()

    quote do
      for family <- unquote(families) do
        @families family
      end
    end
  end

  defmacro promoted(key, value) do
    module = Macro.expand(value, __CALLER__)

    base_type =
      module
      |> apply(:__alchemy_schema__, [])
      |> Map.from_struct()
      |> Macro.escape()

    quote do
      @promoted {[var!(name), unquote(key)], unquote(module)}
      type = unquote(base_type) |> get_in([var!(name), unquote(key)])
      var!(columns) = [{unquote(key), type} | var!(columns)]
    end
  end

  @doc """
  Defines a column family inside a `Bigtable.Schema.row/2` definition.

  The name of the family should be provided to the macro as an atom.

  The block of the macro should only contain `Bigtable.Schema.column/2` definitions.
  """
  defmacro family(name, do: block) do
    quote do
      var!(name) = unquote(name)
      var!(columns) = []
      unquote(block)
      @families {var!(name), Map.new(var!(columns))}
    end
  end

  @doc """
  Defines a column inside a `Bigtable.Schema.family/2` definition.

  The first argument is an atom that will define the column's name.

  The second argument defines the column's type and should be one of:
  - `:integer`
  - `:float`
  - `:boolean`
  - `:string`
  - `:map`
  - `:list`

  If the column value is defined as either `:map` or `:list`, the value will be JSON encoded during mutations and decoded during reads.
  """
  defmacro column(key, {:__aliases__, _, _} = value) do
    type =
      Macro.expand(value, __CALLER__)
      |> apply(:type, [])
      |> Macro.escape()

    c = {key, type}

    quote do
      var!(columns) = [unquote(c) | var!(columns)]
    end
  end

  defmacro column(key, value) do
    c = {key, value}

    quote do
      var!(columns) = [unquote(c) | var!(columns)]
    end
  end
end
