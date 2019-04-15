defmodule MutationsTest do
  alias AlchemyTable.Mutations

  use ExUnit.Case

  describe "Typed.Mutations.create_mutations" do
    setup do
      timestamp = "2019-01-01T00:00:00Z"
      {:ok, datetime, _} = DateTime.from_iso8601(timestamp)

      unix = DateTime.to_unix(datetime, :millisecond) * 1000

      [
        row_key: "Test#1",
        type_spec: %{
          test_family: %{
            test_column: :boolean,
            test_nested: %{
              nested_a: :boolean,
              nested_b: :integer,
              double_nested: %{
                double_nested_a: :boolean,
                double_nested_b: :integer
              }
            }
          }
        },
        timestamp: timestamp,
        unix: unix
      ]
    end

    test "should create mutations for a nested map", context do
      map = %{
        test_family: %{
          test_column: false,
          test_nested: %{
            nested_a: true,
            nested_b: 2,
            double_nested: %{
              double_nested_a: true,
              double_nested_b: 2
            }
          }
        }
      }

      expected = expected_entry("true", "2", context.unix)

      result =
        Mutations.create_mutations(context.row_key, context.type_spec, map, context.timestamp)

      assert result == expected
    end

    test "should create nil properties for nil map value", context do
      map = %{
        test_family: %{
          test_column: false,
          test_nested: nil
        }
      }

      expected = %Google.Bigtable.V2.MutateRowsRequest.Entry{
        row_key: "Test#1",
        mutations: [
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:set_cell,
               %Google.Bigtable.V2.Mutation.SetCell{
                 column_qualifier: "test_column",
                 family_name: "test_family",
                 timestamp_micros: context.unix,
                 value: "false"
               }}
          },
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:delete_from_column,
               %Google.Bigtable.V2.Mutation.DeleteFromColumn{
                 column_qualifier: "test_nested.double_nested.double_nested_a",
                 family_name: "test_family",
                 time_range: %Google.Bigtable.V2.TimestampRange{
                   end_timestamp_micros: 0,
                   start_timestamp_micros: 0
                 }
               }}
          },
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:delete_from_column,
               %Google.Bigtable.V2.Mutation.DeleteFromColumn{
                 column_qualifier: "test_nested.double_nested.double_nested_b",
                 family_name: "test_family",
                 time_range: %Google.Bigtable.V2.TimestampRange{
                   end_timestamp_micros: 0,
                   start_timestamp_micros: 0
                 }
               }}
          },
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:delete_from_column,
               %Google.Bigtable.V2.Mutation.DeleteFromColumn{
                 column_qualifier: "test_nested.nested_a",
                 family_name: "test_family",
                 time_range: %Google.Bigtable.V2.TimestampRange{
                   end_timestamp_micros: 0,
                   start_timestamp_micros: 0
                 }
               }}
          },
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:delete_from_column,
               %Google.Bigtable.V2.Mutation.DeleteFromColumn{
                 column_qualifier: "test_nested.nested_b",
                 family_name: "test_family",
                 time_range: %Google.Bigtable.V2.TimestampRange{
                   end_timestamp_micros: 0,
                   start_timestamp_micros: 0
                 }
               }}
          }
        ]
      }

      result =
        Mutations.create_mutations(context.row_key, context.type_spec, map, context.timestamp)

      assert result == expected
    end
  end

  defp expected_entry(a_value, b_value, unix) do
    %Google.Bigtable.V2.MutateRowsRequest.Entry{
      mutations: [
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "test_column",
               family_name: "test_family",
               timestamp_micros: unix,
               value: "false"
             }}
        },
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "test_nested.double_nested.double_nested_a",
               family_name: "test_family",
               timestamp_micros: unix,
               value: a_value
             }}
        },
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "test_nested.double_nested.double_nested_b",
               family_name: "test_family",
               timestamp_micros: unix,
               value: b_value
             }}
        },
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "test_nested.nested_a",
               family_name: "test_family",
               timestamp_micros: unix,
               value: a_value
             }}
        },
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "test_nested.nested_b",
               family_name: "test_family",
               timestamp_micros: unix,
               value: b_value
             }}
        }
      ],
      row_key: "Test#1"
    }
  end
end
