defmodule AlchemyTable.Type do
  defmacro __using__(_opts) do
    quote do
      import unquote(__MODULE__)
    end
  end

  defmacro type(do: block) do
    quote do
      var!(fields) = []
      unquote(block)

      defstruct var!(fields)

      def type do
        %__MODULE__{}
      end
    end
  end

  defmacro field(key, {:__aliases__, _, _} = value) do
    type =
      value
      |> Macro.expand(__CALLER__)
      |> apply(:type, [])
      |> Macro.escape()

    c = {key, type}

    quote do
      var!(fields) = [unquote(c) | var!(fields)]
    end
  end

  defmacro field(key, value) do
    c = {key, value}

    quote do
      var!(fields) = [unquote(c) | var!(fields)]
    end
  end
end
