defmodule FlatType do
  use AlchemyTable.Type

  type do
    field(:a, :string)
    field(:b, :boolean)
  end
end

defmodule NestedType do
  use AlchemyTable.Type

  type do
    field(:c, :integer)
    field(:nested, FlatType)
  end
end

defmodule StandardTable do
  use AlchemyTable.Table

  @cloned [
    ClonedTable
  ]

  table :standard_table, row_key: "TABLE#[family.id]" do
    family :family do
      column(:a, :integer)
      column(:nested, NestedType)
    end
  end
end

defmodule TSTable do
  use AlchemyTable.Table

  table :ts_table, row_key: "TABLE#[family.id]", ts: true do
    family :family do
      column(:a, :string)
      column(:nested, NestedType)
    end
  end
end

defmodule ClonedTable do
  use AlchemyTable.Table

  table :cloned_table, row_key: "CLONED#[family.id]" do
    clone(StandardTable)
  end
end
