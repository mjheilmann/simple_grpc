defmodule SimpleGrpc.StrictDecodeTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias SimpleGrpc.Protobuf

  defmodule DocsServer do
    use SimpleGrpc.Server, files: ["test/fixtures/protos/strict_decode/strict.proto"]

    def echo(req, _ctx), do: {:ok, req}
  end

  @bad_utf8 <<0xFF, 0xFE, "x">>

  setup_all do
    schema = DocsServer.__schema__()
    {:ok, loose} = SimpleGrpc.Schema.from_file("test/fixtures/protos/strict_decode/strict2.proto")
    assert schema.compiled_module
    assert loose.syntax == :proto2

    %{
      codecs: [compiled: schema, interpreted: %{schema | compiled_module: nil}],
      loose_codecs: [compiled: loose, interpreted: %{loose | compiled_module: nil}]
    }
  end

  for codec <- [:compiled, :interpreted] do
    describe "#{codec} codec: malformed input is a decode error, not a crash" do
      test "varints are at most ten bytes", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert {:ok, %{big: 18_446_744_073_709_551_615}} =
                 decode(field(9, 0, ten_byte_max()), schema)

        assert {:error, 3, _} = decode(field(9, 0, :binary.copy(<<0xFF>>, 11) <> <<1>>), schema)
        # a tag that is itself too long
        assert {:error, 3, _} = decode(:binary.copy(<<0xFF>>, 11) <> <<1>>, schema)
      end

      test "a length prefix past the end of the data", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert {:error, 3, _} = decode(field(1, 2, varint(50) <> "short"), schema)
        assert {:error, 3, _} = decode(field(3, 2, varint(50) <> "short"), schema)
        assert {:error, 3, _} = decode(field(5, 2, varint(1 <<< 31)), schema)
        assert {:error, 3, _} = decode(field(99, 2, varint(50) <> "short"), schema)
      end

      test "truncated tag, varint and fixed-width unknown fields", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert {:error, 3, _} = decode(<<0x80>>, schema)
        assert {:error, 3, _} = decode(field(2, 0, <<0x80>>), schema)
        assert {:error, 3, _} = decode(tag(99, 5) <> <<1, 2>>, schema)
        assert {:error, 3, _} = decode(tag(99, 1) <> <<1, 2, 3>>, schema)
      end

      test "wire types 6 and 7, a stray end-group, and field number 0", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert {:error, 3, _} = decode(tag(99, 6), schema)
        assert {:error, 3, _} = decode(tag(99, 7), schema)
        assert {:error, 3, _} = decode(tag(99, 4), schema)
        # also on a known field number
        assert {:error, 3, _} = decode(tag(2, 6), schema)
        assert {:error, 3, _} = decode(<<0, 1>>, schema)
        assert {:error, 3, _} = decode(field(0, 2, varint(1) <> "x"), schema)
      end

      test "an unterminated group", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert {:error, 3, _} = decode(tag(99, 3), schema)
      end

      test "a well-formed group of unknown fields is skipped", %{codecs: codecs} do
        schema = codecs[unquote(codec)]
        group = tag(97, 3) <> field(1, 0, varint(1)) <> tag(97, 4)

        assert {:ok, %{title: "a"}} = decode(group <> field(1, 2, varint(1) <> "a"), schema)
      end
    end

    describe "#{codec} codec: strings" do
      test "must be valid UTF-8", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert {:ok, %{title: "日本"}} = decode(field(1, 2, len("日本")), schema)
        assert {:error, 3, _} = decode(field(1, 2, len(@bad_utf8)), schema)
        assert {:error, 3, _} = decode(field(20, 2, len(@bad_utf8)), schema)

        assert {:error, 3, _} =
                 decode(field(5, 2, len("ok")) <> field(5, 2, len(@bad_utf8)), schema)

        assert {:error, 3, _} = decode(field(3, 2, field(1, 2, len(@bad_utf8))), schema)
      end

      test "map keys and values too", %{codecs: codecs} do
        schema = codecs[unquote(codec)]
        entry = fn k, v -> field(7, 2, len(field(1, 2, len(k)) <> field(2, 2, len(v)))) end

        assert {:ok, %{labels: %{"k" => "v"}}} = decode(entry.("k", "v"), schema)
        assert {:error, 3, _} = decode(entry.(@bad_utf8, "v"), schema)
        assert {:error, 3, _} = decode(entry.("k", @bad_utf8), schema)
      end

      test "bytes fields are not checked", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert {:ok, %{blob: @bad_utf8}} = decode(field(8, 2, len(@bad_utf8)), schema)
      end
    end

    describe "#{codec} codec: a known field with the wrong wire type" do
      # Reference parsers (protoc) keep the field as unknown and carry on; they do not fail
      # the message. The sibling field after it must still decode.
      test "is skipped, whatever its type", %{codecs: codecs} do
        schema = codecs[unquote(codec)]
        tail = field(1, 2, len("after"))

        for wrong <- [
              # string sent as a varint, a fixed32 and a fixed64
              field(1, 0, varint(5)),
              tag(1, 5) <> <<1, 2, 3, 4>>,
              tag(1, 1) <> <<1, 2, 3, 4, 5, 6, 7, 8>>,
              # int32 sent length-delimited (not a repeated field, so not packed)
              field(2, 2, len("x")),
              # message and map sent as a varint
              field(3, 0, varint(1)),
              field(7, 0, varint(1)),
              # enum sent length-delimited
              field(4, 2, len("x")),
              # a repeated string sent as a varint
              field(5, 0, varint(1)),
              # multi-byte tag
              field(20, 0, varint(1))
            ] do
          assert {:ok, %{title: "after", count: 0, inner: nil, tags: [], labels: labels}} =
                   decode(wrong <> tail, schema)

          assert labels == %{}
        end
      end

      test "repeated scalars accept both the packed and the one-per-tag forms", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert {:ok, %{nums: [1, 2, 3]}} =
                 decode(field(6, 2, len(varint(1) <> varint(2) <> varint(3))), schema)

        assert {:ok, %{nums: [1, 2]}} =
                 decode(field(6, 0, varint(1)) <> field(6, 0, varint(2)), schema)
      end
    end

    describe "#{codec} codec: proto2" do
      test "does not enforce UTF-8, as the reference parsers only warn", %{loose_codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert {:ok, %{title: @bad_utf8}} =
                 Protobuf.decode("strict2.Loose", field(1, 2, len(@bad_utf8)), schema)
      end
    end
  end

  defp tag(num, wire_type), do: varint(num <<< 3 ||| wire_type)

  defp ten_byte_max, do: varint(18_446_744_073_709_551_615)

  defp field(num, wire_type, payload), do: varint(num <<< 3 ||| wire_type) <> payload

  defp len(bin), do: varint(byte_size(bin)) <> bin

  defp varint(value), do: IO.iodata_to_binary(Protobuf.encode_varint(value))

  defp decode(bytes, schema), do: Protobuf.decode("strict.Doc", bytes, schema)
end
