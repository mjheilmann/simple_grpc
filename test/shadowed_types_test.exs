defmodule SimpleGrpc.ShadowedTypesTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Protobuf

  # `shadow.holder.Language` is a message, `shadow.lang.Language` an enum. Linking the files one
  # at a time cannot see the enum from the other file yet; the finished schema must still encode
  # and decode through the right type in both codecs, at every nesting level.
  defmodule HoldingServer do
    use SimpleGrpc.Server,
      files: [
        "test/fixtures/protos/shadowed_types/holder.proto",
        "test/fixtures/protos/shadowed_types/lang.proto"
      ]

    def hold(req, _ctx), do: {:ok, req}
  end

  setup_all do
    schema = HoldingServer.__schema__()
    assert schema.compiled_module
    %{codecs: [compiled: schema, interpreted: %{schema | compiled_module: nil}]}
  end

  defp encode(message, data, schema),
    do: message |> Protobuf.encode!(data, schema) |> IO.iodata_to_binary()

  for codec <- [:compiled, :interpreted] do
    describe "#{codec} codec" do
      test "an enum named like a nearby message is encoded as the enum", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert encode("shadow.holder.Language", %{locale: :LANGUAGE_VAI}, schema) == <<8, 202, 3>>
      end

      test "the same holds when the message is nested in another", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert encode("shadow.holder.Holder", %{language: %{locale: :LANGUAGE_VAI}}, schema) ==
                 <<10, 3, 8, 202, 3>>

        assert encode(
                 "shadow.holder.Holder",
                 %{more: [%{locale: :LANGUAGE_EN}, %{label: "x"}]},
                 schema
               ) == <<18, 2, 8, 1, 18, 3, 18, 1, ?x>>
      end

      test "and decodes back to the enum atom", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert %{language: %{locale: :LANGUAGE_VAI}} =
                 Protobuf.decode!("shadow.holder.Holder", <<10, 3, 8, 202, 3>>, schema)
      end
    end
  end
end
