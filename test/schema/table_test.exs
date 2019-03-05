Code.require_file("../support/test_schema.ex", __DIR__)

defmodule TableTest do
  alias AlchemyTable.Table
  alias Google.Bigtable.V2.MutateRowRequest
  use ExUnit.Case

  doctest Table

  describe "Table.__alchemy_metadata__/0" do
    test "should have the correct metadata for standard tables" do
      expected = %{
        name: :standard_table,
        instance: Bigtable.Utils.configured_instance_name(),
        cloned: [ClonedTable],
        opts: [row_key: "TABLE#[family.id]"],
        schema: %StandardTable{
          family: %{
            a: :integer,
            id: :string,
            nested: %NestedType{
              c: :integer,
              nested: %FlatType{
                a: :string,
                b: :boolean
              }
            }
          }
        },
        promoted: [],
        key_parts: ["TABLE", [:family, :id]],
        table_name: :standard_table,
        full_name: Bigtable.Utils.configured_instance_name() <> "/tables/standard-table"
      }

      assert StandardTable.__alchemy_metadata__() == expected
    end

    test "should have the correct metadata for TS tables" do
      expected = %{
        name: :ts_table,
        instance: Bigtable.Utils.configured_instance_name(),
        cloned: [],
        opts: [row_key: "TABLE#[family.id]", ts: true],
        schema: %TSTable{
          family: %{
            a: :integer,
            id: :string,
            nested: %NestedType{
              c: :integer,
              nested: %FlatType{
                a: :string,
                b: :boolean
              }
            }
          }
        },
        promoted: [],
        key_parts: ["TABLE", [:family, :id]],
        table_name: :ts_table,
        full_name: Bigtable.Utils.configured_instance_name() <> "/tables/ts-table"
      }

      assert TSTable.__alchemy_metadata__() == expected
    end

    test "should have the correct metadata for cloned tables" do
      expected = %{
        name: :cloned_table,
        instance: Bigtable.Utils.configured_instance_name(),
        cloned: [],
        opts: [row_key: "CLONED#[family.id]"],
        schema: %ClonedTable{
          family: %{
            a: :integer,
            id: :string,
            nested: %NestedType{
              c: :integer,
              nested: %FlatType{
                a: :string,
                b: :boolean
              }
            }
          }
        },
        promoted: [],
        key_parts: ["CLONED", [:family, :id]],
        table_name: :cloned_table,
        full_name: Bigtable.Utils.configured_instance_name() <> "/tables/cloned-table"
      }

      assert ClonedTable.__alchemy_metadata__() == expected
    end

    test "should have the correct metadata for promoted tables" do
      expected = %{
        name: :with_promoted,
        instance: Bigtable.Utils.configured_instance_name(),
        cloned: [],
        opts: [row_key: "TABLE#[family_a.id]"],
        schema: %WithPromoted{
          family_a: %{
            id: :string,
            single: :string
          },
          family_b: %{
            nested: %NestedType{
              c: :integer,
              nested: %FlatType{
                a: :string,
                b: :boolean
              }
            }
          }
        },
        promoted: [
          {[:family_b, :nested], NestedValuePromoted},
          {[:family_a, :single], SingleValuePromoted}
        ],
        key_parts: ["TABLE", [:family_a, :id]],
        table_name: :with_promoted,
        full_name: Bigtable.Utils.configured_instance_name() <> "/tables/with-promoted"
      }

      assert WithPromoted.__alchemy_metadata__() == expected
    end
  end
end
