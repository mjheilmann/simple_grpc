defmodule SimpleGrpc.IntWidthsTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias SimpleGrpc.Protobuf

  defmodule WidthsServer do
    use SimpleGrpc.Server, files: ["test/fixtures/protos/int_widths/int_widths.proto"]

    def echo(req, _ctx), do: {:ok, req}
  end

  @msg "intwidths.Ints"
  @int32_min -2_147_483_648
  @int32_max 2_147_483_647
  @int64_min -9_223_372_036_854_775_808
  @int64_max 9_223_372_036_854_775_807
  @uint64_max 18_446_744_073_709_551_615

  setup_all do
    schema = WidthsServer.__schema__()
    assert schema.compiled_module
    %{codecs: [compiled: schema, interpreted: %{schema | compiled_module: nil}]}
  end

  for codec <- [:compiled, :interpreted] do
    describe "#{codec} codec" do
      test "negative int32 and int64 are ten-byte varints and round-trip", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        # tag 0x08 then the two's complement of -2 as a ten-byte varint
        assert encode(%{i32: -2}, schema) ==
                 <<8, 254, 255, 255, 255, 255, 255, 255, 255, 255, 1>>

        for v <- [-1, -2, @int32_min, @int32_max] do
          assert decode(encode(%{i32: v}, schema), schema).i32 == v
        end

        for v <- [-1, @int64_min, @int64_max] do
          assert decode(encode(%{i64: v}, schema), schema).i64 == v
        end
      end

      test "decodes a negative value sent as a ten-byte varint", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert decode(field(1, -2), schema).i32 == -2
        assert decode(field(2, -2), schema).i64 == -2
      end

      test "truncates a varint wider than the field, as the reference parsers do", %{
        codecs: codecs
      } do
        schema = codecs[unquote(codec)]

        assert decode(field(1, (1 <<< 32) + 5), schema).i32 == 5
        assert decode(field(1, (1 <<< 32) + 0xFFFFFFFF), schema).i32 == -1
        assert decode(field(3, (1 <<< 32) + 5), schema).u32 == 5
        assert decode(field(4, (1 <<< 64) + 3), schema).u64 == 3
      end

      test "unsigned values at their limits are untouched", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert decode(field(3, 0xFFFFFFFF), schema).u32 == 0xFFFFFFFF
        assert decode(field(4, @uint64_max), schema).u64 == @uint64_max
        assert decode(encode(%{u64: @uint64_max}, schema), schema).u64 == @uint64_max
      end

      test "repeated, packed and not", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        decoded =
          decode(
            encode(
              %{
                packed_i32: [-1, 1, @int32_min],
                loose_i64: [-1, 300, @int64_min],
                packed_u32: [1, 2]
              },
              schema
            ),
            schema
          )

        assert decoded.packed_i32 == [-1, 1, @int32_min]
        assert decoded.loose_i64 == [-1, 300, @int64_min]
        assert decoded.packed_u32 == [1, 2]
      end

      test "repeated elements are converted on the unpacked and the packed wire forms", %{
        codecs: codecs
      } do
        schema = codecs[unquote(codec)]

        unpacked = field(5, -2) <> field(5, 7)
        packed = <<42, 11>> <> varint(-2) <> varint(7)

        assert decode(unpacked, schema).packed_i32 == [-2, 7]
        assert decode(packed, schema).packed_i32 == [-2, 7]
      end

      test "map keys and values", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        decoded = decode(encode(%{table: %{-5 => -6, 5 => @int64_min}}, schema), schema)
        assert decoded.table == %{-5 => -6, 5 => @int64_min}
      end

      test "a field with a multi-byte tag and a oneof member", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        decoded = decode(encode(%{far_i32: -3}, schema), schema)
        assert decoded.far_i32 == -3

        # field 9 (oneof member), ten-byte varint
        assert inspect(decode(field(9, -3), schema)) =~ "-3"
      end
    end
  end

  defp field(num, value), do: <<num <<< 3, varint(value)::binary>>

  defp varint(value), do: IO.iodata_to_binary(Protobuf.encode_varint(value))

  defp encode(data, schema), do: IO.iodata_to_binary(Protobuf.encode!(@msg, data, schema))

  defp decode(bytes, schema), do: Protobuf.decode!(@msg, bytes, schema)
end
