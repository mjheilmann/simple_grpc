defmodule SimpleGrpc.EmptySubmessageTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Protobuf

  defmodule MarkingServer do
    use SimpleGrpc.Server, files: ["test/fixtures/protos/empty_message/empty_message.proto"]

    def mark(req, _ctx), do: {:ok, req}
  end

  setup_all do
    schema = MarkingServer.__schema__()
    assert schema.compiled_module
    %{codecs: [compiled: schema, interpreted: %{schema | compiled_module: nil}]}
  end

  defp encode(data, schema),
    do: "emptymsg.Outer" |> Protobuf.encode!(data, schema) |> IO.iodata_to_binary()

  for codec <- [:compiled, :interpreted] do
    describe "#{codec} codec" do
      test "a set empty message is written, an unset one is not", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert encode(%{marker: %{}}, schema) == <<10, 0>>
        assert encode(%{marker: nil}, schema) == <<>>
        assert encode(%{}, schema) == <<>>
      end

      test "an empty message among other fields keeps its place", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert encode(%{marker: %{}, inner: %{name: "x"}}, schema) == <<10, 0, 18, 3, 10, 1, ?x>>
        assert encode(%{inner: %{}}, schema) == <<18, 0>>
      end

      test "repeated, oneof and map values may be empty messages", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert encode(%{markers: [%{}, %{}]}, schema) == <<26, 0, 26, 0>>
        assert encode(%{picked: %{}}, schema) == <<34, 0>>
        assert encode(%{tagged: %{"a" => %{}}}, schema) == <<50, 5, 10, 1, ?a, 18, 0>>
      end

      test "presence survives a round trip", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert %{marker: marker, inner: nil} =
                 Protobuf.decode!("emptymsg.Outer", <<10, 0>>, schema)

        refute is_nil(marker)

        assert %{marker: nil} = Protobuf.decode!("emptymsg.Outer", <<>>, schema)

        decoded = Protobuf.decode!("emptymsg.Outer", <<10, 0, 26, 0>>, schema)
        assert encode(decoded, schema) == <<10, 0, 26, 0>>
      end
    end
  end
end
