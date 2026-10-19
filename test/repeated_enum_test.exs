defmodule SimpleGrpc.RepeatedEnumTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Protobuf

  defmodule PainterServer do
    use SimpleGrpc.Server, files: ["test/fixtures/protos/repeated_enum/repeated_enum.proto"]

    def mix(_req, _ctx), do: {:ok, %{}}
  end

  setup_all do
    schema = PainterServer.__schema__()
    assert schema.compiled_module
    %{codecs: [compiled: schema, interpreted: %{schema | compiled_module: nil}]}
  end

  for codec <- [:compiled, :interpreted] do
    describe "#{codec} codec" do
      test "packs repeated enums by default", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        # field 1, length-delimited, two varints
        assert encode("repenum.Palette", %{colors: [:RED, :BLUE]}, schema) == <<10, 2, 1, 2>>
        assert encode("repenum.Palette", %{shades: [:DARK, :LIGHT]}, schema) == <<18, 2, 2, 1>>
      end

      test "writes one tagged varint per element when not packed", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert encode("repenum.Palette", %{loose: [:RED, :BLUE]}, schema) == <<24, 1, 24, 2>>
      end

      test "accepts atoms, strings and integers per element", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert encode("repenum.Palette", %{colors: [:RED, "BLUE", 1]}, schema) ==
                 <<10, 3, 1, 2, 1>>
      end

      test "round-trips, including inside a nested message", %{codecs: codecs} do
        schema = codecs[unquote(codec)]
        palette = %{colors: [:RED, :BLUE, :RED], shades: [:DARK], loose: [:BLUE, :RED], name: "x"}

        decoded =
          "repenum.Gallery"
          |> encode(%{palettes: [palette, %{colors: [:BLUE]}]}, schema)
          |> then(&Protobuf.decode!("repenum.Gallery", &1, schema))

        assert [first, second] = decoded.palettes
        assert first.colors == [:RED, :BLUE, :RED]
        assert first.shades == [:DARK]
        assert first.loose == [:BLUE, :RED]
        assert first.name == "x"
        assert second.colors == [:BLUE]
        assert second.shades == []
      end

      test "decodes both packed and unpacked wire forms", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        # `colors` (packed field) sent unpacked, `loose` (unpacked field) sent packed
        assert %{colors: [:RED, :BLUE]} =
                 Protobuf.decode!("repenum.Palette", <<8, 1, 8, 2>>, schema)

        assert %{loose: [:BLUE, :RED]} =
                 Protobuf.decode!("repenum.Palette", <<26, 2, 2, 1>>, schema)
      end

      test "omits an empty list", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert encode("repenum.Palette", %{colors: [], shades: []}, schema) == <<>>
      end
    end
  end

  defp encode(message, data, schema),
    do: IO.iodata_to_binary(Protobuf.encode!(message, data, schema))
end
