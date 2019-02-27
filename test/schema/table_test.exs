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
        promoted: []
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
        promoted: []
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
        promoted: []
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
        ]
      }

      assert WithPromoted.__alchemy_metadata__() == expected
    end
  end

  describe "Table.update/2" do
    setup do
      [
        standard_data: %{
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
        },
        standard_mutations: [
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:set_cell,
               %Google.Bigtable.V2.Mutation.SetCell{
                 column_qualifier: "a",
                 family_name: "family",
                 timestamp_micros: -1,
                 value: "1"
               }}
          },
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:set_cell,
               %Google.Bigtable.V2.Mutation.SetCell{
                 column_qualifier: "id",
                 family_name: "family",
                 timestamp_micros: -1,
                 value: "id-1"
               }}
          },
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:set_cell,
               %Google.Bigtable.V2.Mutation.SetCell{
                 column_qualifier: "nested.c",
                 family_name: "family",
                 timestamp_micros: -1,
                 value: "2"
               }}
          },
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:set_cell,
               %Google.Bigtable.V2.Mutation.SetCell{
                 column_qualifier: "nested.nested.a",
                 family_name: "family",
                 timestamp_micros: -1,
                 value: "value"
               }}
          },
          %Google.Bigtable.V2.Mutation{
            mutation:
              {:set_cell,
               %Google.Bigtable.V2.Mutation.SetCell{
                 column_qualifier: "nested.nested.b",
                 family_name: "family",
                 timestamp_micros: -1,
                 value: "true"
               }}
          }
        ]
      ]
    end

    test "should build request for standard table with cloned table", context do
      standard_request = %MutateRowRequest{
        app_profile_id: "",
        mutations: context.standard_mutations,
        row_key: "TABLE#id-1",
        table_name: Bigtable.Utils.configured_instance_name() <> "/tables/standard-table"
      }

      cloned_request = %MutateRowRequest{
        app_profile_id: "",
        mutations: context.standard_mutations,
        row_key: "CLONED#id-1",
        table_name: Bigtable.Utils.configured_instance_name() <> "/tables/cloned-table"
      }

      expected = [
        standard_request,
        cloned_request
      ]

      assert StandardTable.update(context.standard_data) == expected
    end

    test "should build request for ts table", context do
      expected = [
        %MutateRowRequest{
          app_profile_id: "",
          mutations: context.standard_mutations,
          row_key: "TABLE#id-1#timestamp-now",
          table_name: Bigtable.Utils.configured_instance_name() <> "/tables/ts-table"
        }
      ]

      assert TSTable.update(context.standard_data, "timestamp-now") == expected
    end

    test "should be request for promoted tables" do
      single_mutations = [
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "single",
               family_name: "family_a",
               timestamp_micros: -1,
               value: "1"
             }}
        }
      ]

      single_request = %MutateRowRequest{
        app_profile_id: "",
        mutations: single_mutations,
        row_key: "TABLE#id-1",
        table_name: Bigtable.Utils.configured_instance_name() <> "/tables/single-value-promoted"
      }

      nested_mutations = [
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "nested.c",
               family_name: "family_b",
               timestamp_micros: -1,
               value: "1"
             }}
        },
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "nested.nested.a",
               family_name: "family_b",
               timestamp_micros: -1,
               value: "value"
             }}
        },
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "nested.nested.b",
               family_name: "family_b",
               timestamp_micros: -1,
               value: "true"
             }}
        }
      ]

      nested_request = %MutateRowRequest{
        app_profile_id: "",
        mutations: nested_mutations,
        row_key: "TABLE#id-1",
        table_name: Bigtable.Utils.configured_instance_name() <> "/tables/nested-value-promoted"
      }

      main_mutations = [
        %Google.Bigtable.V2.Mutation{
          mutation:
            {:set_cell,
             %Google.Bigtable.V2.Mutation.SetCell{
               column_qualifier: "id",
               family_name: "family_a",
               timestamp_micros: -1,
               value: "id-1"
             }}
        }
      ]

      main_request = %MutateRowRequest{
        app_profile_id: "",
        mutations: main_mutations ++ single_mutations ++ nested_mutations,
        row_key: "TABLE#id-1",
        table_name: Bigtable.Utils.configured_instance_name() <> "/tables/with-promoted"
      }

      expected = [
        main_request,
        nested_request,
        single_request
      ]

      data = %{
        family_a: %{
          id: "id-1",
          single: "1"
        },
        family_b: %{
          nested: %{
            c: 1,
            nested: %{
              a: "value",
              b: true
            }
          }
        }
      }

      assert WithPromoted.update(data) == expected
    end
  end
end
