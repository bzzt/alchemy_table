defmodule SchemaTest do
  use ExUnit.Case

  defmodule OneColumnType do
    use AlchemyTable.Type

    type do
      field(:a, :integer)
    end
  end

  defmodule TwoColumnType do
    use AlchemyTable.Type

    type do
      field(:a, :integer)
      field(:b, :boolean)
    end
  end

  defmodule TestSchema do
    use AlchemyTable.Table

    table :entity, row_key: "ENTITY#[family_a.a]" do
      family :family_a do
        column(:a, :string)
        column(:b, :map)
      end
    end
  end

  defmodule TestSchemaWithType do
    use AlchemyTable.Table

    table :entity, row_key: "ENTITY#[family_a.a]" do
      family :family_a do
        column(:a, :string)
        column(:b, :map)
      end

      family :family_b do
        column(:a, SchemaTest.OneColumnType)
        column(:b, SchemaTest.TwoColumnType)
      end
    end
  end

  describe "Schema - Type" do
    test "should generate a type with multiple columns" do
      expected = %SchemaTest.TwoColumnType{
        a: :integer,
        b: :boolean
      }

      assert SchemaTest.TwoColumnType.type() == expected
    end

    test "should generate a type with a single column" do
      expected = %SchemaTest.OneColumnType{
        a: :integer
      }

      assert SchemaTest.OneColumnType.type() == expected
    end
  end

  describe "Schema - Row" do
    test "should generate a row with scalar types" do
      expected = %SchemaTest.TestSchema{
        family_a: %{
          a: :string,
          b: :map
        }
      }

      assert SchemaTest.TestSchema.__alchemy_schema__() == expected
    end
  end

  test "should generate a row with schema types" do
    expected = %SchemaTest.TestSchemaWithType{
      family_a: %{
        a: :string,
        b: :map
      },
      family_b: %{
        a: %SchemaTest.OneColumnType{
          a: :integer
        },
        b: %SchemaTest.TwoColumnType{
          a: :integer,
          b: :boolean
        }
      }
    }

    assert SchemaTest.TestSchemaWithType.__alchemy_schema__() == expected
  end
end
