defmodule AlchemyTable.Table do
  defmacro __using__(_opt) do
    quote do
      alias AlchemyTable.Operations.Update
      import unquote(__MODULE__)
      import AlchemyTable.Table.Utils
      Module.register_attribute(__MODULE__, :families, accumulate: true)
      Module.register_attribute(__MODULE__, :promoted, accumulate: true)
      Module.register_attribute(__MODULE__, :cloned, accumulate: true)
    end
  end

  defmacro table(name, opts, do: block) do
    instance = Bigtable.Utils.configured_instance_name()

    quote do
      @key_parts unquote(opts) |> get_key_pattern!() |> build_key_parts()
      unquote(block)
      defstruct @families

      def metadata do
        %{
          name: unquote(name),
          instance: unquote(instance),
          cloned: @cloned,
          promoted: @promoted,
          opts: unquote(opts),
          schema: __alchemy_schema__()
        }
      end

      def __alchemy_schema__ do
        %__MODULE__{}
      end

      def build_updates(data) do
        %{cloned: cloned, promoted: promoted, instance: instance, schema: schema} = metadata()
        cloned = cloned |> List.flatten()

        main_key =
          build_row_key(@key_parts, data)
          |> add_ts(unquote(opts))

        main_update =
          main_key
          |> AlchemyTable.Operations.Update.update(schema, data)

        cloned_updates =
          for table <- List.flatten(cloned), into: [] do
            meta = table.metadata()
            %{name: table_name, instance: instance, opts: opts} = meta
            update = clone_update(main_key, main_update, data, opts)
            {instance, table_name, update}
          end

        promoted_updates =
          for {column, module} <- promoted,
              get_in(data, column) != nil,
              into: [] do
            apply(module, :build_updates, [data])
          end

        [{instance, unquote(name), main_update}, cloned_updates, promoted_updates]
        |> List.flatten()
      end

      def update(data) do
        build_updates(data)
        |> Enum.map(&build_mutate_row/1)
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
      @families {unquote(name), Map.new(var!(columns))}
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
