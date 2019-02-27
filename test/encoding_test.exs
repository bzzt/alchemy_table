defmodule EncodingTest do
  alias AlchemyTable.Encoding

  use ExUnit.Case

  doctest Encoding

  describe "Encoding.encode/1 - to string" do
    test "should encode integers" do
      assert encode_string(:integer, 1) == "1"
    end

    test "should encode lists" do
      list = [1, 2, 3]
      expected = "\[1,2,3\]"
      assert encode_string(:list, list) == expected
    end

    test "should encode maps" do
      map = %{key: true}
      expected = "{\"key\":true}"
      assert encode_string(:map, map) == expected
    end

    test "should encode floats" do
      assert encode_string(:float, 24.2) == "24.2"
    end

    test "should encode booleans" do
      assert encode_string(:boolean, true) == "true"
      assert encode_string(:boolean, false) == "false"
    end

    test "should encode strings" do
      assert encode_string(:string, "value") == "value"
    end
  end

  describe "Encoding.encode/1 - to bytes" do
    test "should encode integers" do
      expected = <<1::integer-signed-64>>

      assert encode_bytes(:integer, 1) == expected
    end

    test "should encode lists" do
      expected = "\[1,2,3\]"

      assert encode_bytes(:list, [1, 2, 3]) == expected
    end

    test "should encode maps" do
      expected = "{\"key\":true}"

      assert encode_bytes(:map, %{key: true}) == expected
    end

    test "should encode floats" do
      expected = <<24.2::float-signed-64>>

      assert encode_bytes(:float, 24.2) == expected
    end

    test "should encode booleans" do
      t_expected = <<1>>
      f_expected = <<0>>

      assert encode_bytes(:boolean, true) == t_expected
      assert encode_bytes(:boolean, false) == f_expected
    end

    test "should encode strings" do
      expected = <<"value">>

      assert encode_bytes(:string, "value") == expected
    end
  end

  defp encode_string(type, string) do
    Encoding.encode(type, string, mode: :string)
  end

  defp encode_bytes(type, bytes) do
    Encoding.encode(type, bytes, mode: :bytes)
  end
end
