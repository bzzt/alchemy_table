Code.require_file("../support/test_schema.ex", __DIR__)

defmodule TypeTest do
  alias AlchemyTable.Table
  use ExUnit.Case

  doctest Table

  describe "FlatType" do
    test "should have correct type" do
      expected = %FlatType{
        a: :string,
        b: :boolean
      }

      assert FlatType.type() == expected
    end
  end

  describe "NestedType" do
    test "should contain correct typings with a nested type definition" do
      expected = %NestedType{
        c: :integer,
        nested: %FlatType{
          a: :string,
          b: :boolean
        }
      }

      assert NestedType.type() == expected
    end
  end
end
