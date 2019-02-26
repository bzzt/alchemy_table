Code.require_file("../support/test_schema.ex", __DIR__)

defmodule TableTest do
  alias AlchemyTable.Table
  use ExUnit.Case

  doctest Table

  describe "Standard Table" do
    test "should have the correct metadata" do
      expected = %{
        name: :standard_table,
        instance: Bigtable.Utils.configured_instance_name(),
        cloned: [ClonedTable],
        opts: [row_key: "TABLE#[family.id]"],
        schema: %StandardTable{
          family: %{
            a: :integer,
            nested: %NestedType{
              c: :integer,
              nested: %FlatType{
                a: :string,
                b: :boolean
              }
            }
          }
        },
        promoted: []
      }

      assert StandardTable.__alchemy_metadata__() == expected
    end
  end
end
