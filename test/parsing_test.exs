defmodule ParsingTest do
  alias AlchemyTable.Parsing
  alias Bigtable.ChunkReader.ReadCell
  alias Google.Protobuf.{BytesValue, StringValue}
  use ExUnit.Case

  doctest Parsing

  describe "Parsing.parse_rows/2" do
    test "should parse the result of a chunk reader based on a schema" do
      schema = StandardTable.__alchemy_schema__()

      rows = %{
        "TABLE#id-1" => [
          %ReadCell{
            family_name: %StringValue{value: "family"},
            label: "",
            qualifier: %BytesValue{value: "a"},
            row_key: "TABLE#id-1",
            timestamp: 123,
            value: "1"
          },
          %ReadCell{
            family_name: %StringValue{value: "family"},
            label: "",
            qualifier: %BytesValue{value: "id"},
            row_key: "TABLE#id-1",
            timestamp: 123,
            value: "id-1"
          },
          %ReadCell{
            family_name: %StringValue{value: "family"},
            label: "",
            qualifier: %BytesValue{value: "nested.c"},
            row_key: "TABLE#id-1",
            timestamp: 123,
            value: "2"
          },
          %ReadCell{
            family_name: %StringValue{value: "family"},
            label: "",
            qualifier: %BytesValue{value: "nested.nested.a"},
            row_key: "TABLE#id-1",
            timestamp: 123,
            value: "value"
          },
          %ReadCell{
            family_name: %StringValue{value: "family"},
            label: "",
            qualifier: %BytesValue{value: "nested.nested.b"},
            row_key: "TABLE#id-1",
            timestamp: 123,
            value: "true"
          }
        ]
      }

      expected = %{
        "TABLE#id-1" => %{
          family: %{
            a: 1,
            id: "id-1",
            nested: %{
              c: 2,
              nested: %{
                a: "value",
                b: true
              }
            }
          }
        }
      }

      assert Parsing.parse_rows(rows, schema) == expected
    end
  end
end
