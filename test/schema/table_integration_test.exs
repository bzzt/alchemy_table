defmodule TableIntegrationTest do
  alias Bigtable.{Mutations, MutateRow, RowFilter}
  use ExUnit.Case

  describe "Table" do
    setup do
      timestamp = "current-ts"
      row_keys = ["TABLE#1##{timestamp}", "TABLE#2##{timestamp}"]

      on_exit(fn ->
        for row_key <- row_keys do
          drop_row(TSTable, row_key)
        end
      end)

      [timestamp: timestamp]
    end

    test "should write and read a single row", context do
      assert TSTable.get() == {:ok, %{}}

      id = "1"

      data = %{
        family: %{
          a: 1,
          id: id,
          nested: %{
            c: 2,
            nested: %{
              a: "value",
              b: true
            }
          }
        }
      }

      expected = %{
        "TABLE##{id}##{context.timestamp}" => data
      }

      data
      |> TSTable.update(context.timestamp)
      |> Enum.map(&MutateRow.mutate/1)

      {:ok, rows} = TSTable.get()

      assert rows == expected
    end

    test "should write and read multiple rows", context do
      assert TSTable.get() == {:ok, %{}}

      data = [
        %{
          family: %{
            a: 1,
            id: "1",
            nested: %{
              c: 2,
              nested: %{
                a: "value",
                b: true
              }
            }
          }
        },
        %{
          family: %{
            a: 1,
            id: "2",
            nested: %{
              c: 2,
              nested: %{
                a: "value",
                b: true
              }
            }
          }
        }
      ]

      expected = %{
        "TABLE#1##{context.timestamp}" => Enum.at(data, 0),
        "TABLE#2##{context.timestamp}" => Enum.at(data, 1)
      }

      for d <- data do
        d
        |> TSTable.update(context.timestamp)
        |> Enum.map(&MutateRow.mutate/1)
      end

      {:ok, rows} = TSTable.get()

      assert rows == expected
    end

    test "should allow filters to be used", context do
      assert TSTable.get() == {:ok, %{}}

      data = [
        %{
          family: %{
            a: 1,
            id: "1",
            nested: %{
              c: 2,
              nested: %{
                a: "value",
                b: true
              }
            }
          }
        },
        %{
          family: %{
            a: 1,
            id: "2",
            nested: %{
              c: 2,
              nested: %{
                a: "value",
                b: true
              }
            }
          }
        }
      ]

      expected = %{
        "TABLE#2##{context.timestamp}" => Enum.at(data, 1)
      }

      for d <- data do
        d
        |> TSTable.update(context.timestamp)
        |> Enum.map(&MutateRow.mutate/1)
      end

      {:ok, rows} = RowFilter.row_key_regex("^TABLE#2#[a-zA-Z-]{0,}") |> TSTable.get()

      assert rows == expected
    end
  end

  defp drop_row(table, row_key) do
    %{full_name: table_name} = table.__alchemy_metadata__()

    {:ok, _} =
      Mutations.build(row_key)
      |> Mutations.delete_from_row()
      |> MutateRow.build(table_name)
      |> MutateRow.mutate()
  end
end
