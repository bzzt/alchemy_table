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
      column(:id, :string)
      column(:nested, NestedType)
    end
  end
end

defmodule TSTable do
  use AlchemyTable.Table

  table :ts_table, row_key: "TABLE#[family.id]", ts: true do
    family :family do
      column(:a, :integer)
      column(:id, :string)
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

defmodule SingleValuePromoted do
  use AlchemyTable.Table

  table :single_value_promoted, row_key: "TABLE#[family_a.id]" do
    family :family_a do
      column(:single, :string)
    end
  end
end

defmodule NestedValuePromoted do
  use AlchemyTable.Table

  table :nested_value_promoted, row_key: "TABLE#[family_a.id]" do
    family :family_b do
      column(:nested, NestedType)
    end
  end
end

defmodule WithPromoted do
  use AlchemyTable.Table

  table :with_promoted, row_key: "TABLE#[family_a.id]" do
    family :family_a do
      column(:id, :string)
      promoted(:single, SingleValuePromoted)
    end

    family :family_b do
      promoted(:nested, NestedValuePromoted)
    end
  end
end
