defmodule DecodingTest do
  alias AlchemyTable.Decoding

  use ExUnit.Case

  doctest Decoding

  describe "Decoding.decode/1 - from string" do
    test "should decode integers" do
      assert decode_string(:integer, "1") == 1
    end

    test "should decode lists" do
      string = "\[1, 2, 3\]"
      expected = [1, 2, 3]
      assert decode_string(:list, string) == expected
    end

    test "should decode maps" do
      string = "{\"key\": true}"
      expected = %{key: true}
      assert decode_string(:map, string) == expected
    end

    test "should decode floats" do
      assert decode_string(:float, "24.2") == 24.2
    end

    test "should decode booleans" do
      assert decode_string(:boolean, "true") == true
      assert decode_string(:boolean, "false") == false
    end

    test "should decode strings" do
      assert decode_string(:string, "value") == "value"
    end
  end

  describe "Decoding.decode/1 - from bytes" do
    test "should decode integers" do
      result =
        <<1::integer-signed-64>>
        |> decode_bytes(:integer)

      assert result == 1
    end

    test "should decode lists" do
      result =
        "\[1, 2, 3\]"
        |> decode_bytes(:list)

      assert result == [1, 2, 3]
    end

    test "should decode maps" do
      result =
        "{\"key\": true}"
        |> decode_bytes(:map)

      assert result == %{key: true}
    end

    test "should decode floats" do
      result =
        <<24.2::float-signed-64>>
        |> decode_bytes(:float)

      assert result == 24.2
    end

    test "should decode booleans" do
      t_result =
        <<1>>
        |> decode_bytes(:boolean)

      f_result =
        <<0>>
        |> decode_bytes(:boolean)

      assert t_result == true
      assert f_result == false
    end

    test "should decode strings" do
      result =
        <<"value">>
        |> decode_bytes(:string)

      assert result == "value"
    end
  end

  defp decode_string(type, string) do
    Decoding.decode(type, string, mode: :string)
  end

  defp decode_bytes(bytes, type) do
    Decoding.decode(type, bytes, mode: :bytes)
  end
end
