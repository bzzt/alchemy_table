defmodule TableIntegrationTest do
  alias Bigtable.{MutateRow, Mutations, RowFilter}
  use ExUnit.Case

  describe "Table.update/2 - Standard with cloned" do
    setup do
      assert StandardTable.get() == {:ok, %{}}
      assert ClonedTable.get() == {:ok, %{}}

      data = %{
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
        },
        "TABLE#id-2" => %{
          family: %{
            a: 1,
            id: "id-2",
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

      on_exit(fn ->
        for {row_key, row_data} <- data do
          drop_row(StandardTable, row_key)
          cloned_key = "CLONED##{get_in(row_data, [:family, :id])}"
          drop_row(ClonedTable, cloned_key)
        end
      end)

      [data: data]
    end

    test "should write and read a single row to standard and cloned tables", context do
      row_key = "TABLE#id-1"

      data = context.data[row_key]

      data
      |> StandardTable.update()

      {:ok, rows} = StandardTable.get()
      {:ok, cloned_rows} = ClonedTable.get()

      assert rows == %{row_key => data}
      assert cloned_rows == %{"CLONED#id-1" => data}
    end

    test "should write and read multiple rows", context do
      for {_, data} <- context.data do
        data
        |> StandardTable.update()
      end

      {:ok, rows} = StandardTable.get()
      {:ok, cloned_rows} = ClonedTable.get()

      expected_cloned = %{
        "CLONED#id-1" => context.data["TABLE#id-1"],
        "CLONED#id-2" => context.data["TABLE#id-2"]
      }

      assert rows == context.data
      assert cloned_rows == expected_cloned
    end

    test "should allow filters to be used", context do
      row_key = "TABLE#id-1"

      data = context.data[row_key]

      for {_, data} <- context.data do
        data
        |> StandardTable.update()
      end

      {:ok, rows} =
        "^#{row_key}"
        |> RowFilter.row_key_regex()
        |> StandardTable.get()

      assert rows == %{row_key => data}
    end

    test "should return the created row", context do
      row_key = "TABLE#id-1"

      data = context.data[row_key]

      response =
        data
        |> StandardTable.update(return: true)

      assert response.standard_table == {:ok, %{row_key => data}}
      assert response.cloned_table == {:ok, %{"CLONED#id-1" => data}}
    end
  end

  describe "Table.update/2 - TS Table" do
    setup do
      timestamp = "current-ts"
      row_key = "TABLE#id-1##{timestamp}"

      data = %{
        row_key => %{
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

      on_exit(fn ->
        drop_row(TSTable, row_key)
      end)

      [data: data, timestamp: timestamp, row_key: row_key]
    end

    test "should write and read from a ts table", context do
      assert TSTable.get() == {:ok, %{}}

      row_data =
        context.data
        |> Map.get(context.row_key)

      row_data
      |> TSTable.update(timestamp: context.timestamp)

      {:ok, rows} = TSTable.get()

      assert rows == context.data
    end
  end

  describe "Table.update/2 - Promoted Tables" do
    setup do
      row_key = "TABLE#id-1"

      data = %{
        row_key => %{
          family_a: %{
            id: "id-1",
            single: "value"
          },
          family_b: %{
            nested: %{
              c: 1,
              nested: %{
                a: "1",
                b: true
              }
            }
          }
        }
      }

      on_exit(fn ->
        drop_row(WithPromoted, row_key)
        drop_row(SingleValuePromoted, row_key)
        drop_row(NestedValuePromoted, row_key)
      end)

      [data: data, row_key: row_key]
    end

    test "should write and read from promoted and base tables", context do
      assert WithPromoted.get() == {:ok, %{}}
      assert SingleValuePromoted.get() == {:ok, %{}}
      assert NestedValuePromoted.get() == {:ok, %{}}

      row_data =
        context.data
        |> Map.get(context.row_key)

      row_data
      |> WithPromoted.update()

      {:ok, rows} = WithPromoted.get()

      assert rows == context.data
    end
  end

  defp drop_row(table, row_key) do
    %{full_name: table_name} = table.__alchemy_metadata__()

    {:ok, _} =
      row_key
      |> Mutations.build()
      |> Mutations.delete_from_row()
      |> MutateRow.build(table_name)
      |> MutateRow.mutate()
  end
end
