defmodule SimpleGrpc.SubmessageMergeTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Protobuf

  defmodule MergerServer do
    use SimpleGrpc.Server, files: ["test/fixtures/protos/submessage_merge/merge.proto"]

    def echo(req, _ctx), do: {:ok, req}
  end

  setup_all do
    schema = MergerServer.__schema__()
    assert schema.compiled_module
    %{codecs: [compiled: schema, interpreted: %{schema | compiled_module: nil}]}
  end

  # A singular message field that appears more than once on the wire is merged, not replaced:
  # scalars take the last value, repeated fields concatenate, nested messages and maps merge.
  # Concatenating two encoded messages is, by definition, the wire form of merging them.
  for codec <- [:compiled, :interpreted] do
    describe "#{codec} codec" do
      test "fields set in different occurrences are all kept", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        decoded = merged([%{inner: %{body: "b"}}, %{inner: %{count: 7}}], schema)

        assert %{body: "b", count: 7} = decoded.inner
      end

      test "a scalar takes the last value, including an explicit default", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert merged([%{inner: %{count: 5}}, %{inner: %{count: 9}}], schema).inner.count == 9

        # an explicit zero is not encoded by the helper, so write the occurrence by hand
        zero = <<10, 2, 16, 0>>
        bytes = encode(%{inner: %{count: 5}}, schema) <> zero
        assert Protobuf.decode!("merge.Outer", bytes, schema).inner.count == 0
      end

      test "repeated fields concatenate", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        decoded = merged([%{inner: %{tags: ["a", "b"]}}, %{inner: %{tags: ["c"]}}], schema)

        assert decoded.inner.tags == ["a", "b", "c"]
      end

      test "nested messages merge recursively", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        decoded =
          merged([%{inner: %{deep: %{a: "A"}}}, %{inner: %{deep: %{b: "B"}, body: "x"}}], schema)

        assert %{body: "x", deep: %{a: "A", b: "B"}} = decoded.inner
      end

      test "map entries merge, a repeated key takes the last value", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        decoded =
          merged(
            [%{inner: %{labels: %{"k" => "1", "x" => "1"}}}, %{inner: %{labels: %{"k" => "2"}}}],
            schema
          )

        assert decoded.inner.labels == %{"k" => "2", "x" => "1"}
      end

      test "three occurrences, and a field with a multi-byte tag", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        decoded =
          merged(
            [
              %{far_inner: %{body: "b"}},
              %{far_inner: %{count: 1}},
              %{far_inner: %{tags: ["t"]}}
            ],
            schema
          )

        assert %{body: "b", count: 1, tags: ["t"]} = decoded.far_inner
      end

      test "the same oneof member merges, a different member replaces it", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        same =
          merged([%{pick_inner: %{body: "b"}}, %{pick_inner: %{count: 3}}], schema)

        assert %{body: "b", count: 3} = same.pick_inner

        other = merged([%{pick_inner: %{body: "b"}}, %{pick_text: "t"}], schema)

        assert other.pick_text == "t"
        assert other.pick_inner == nil

        back = merged([%{pick_text: "t"}, %{pick_inner: %{body: "b"}}], schema)

        assert back.pick_inner.body == "b"
        assert back.pick_text in [nil, ""]
      end

      test "a single occurrence is unchanged", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        decoded = merged([%{inner: %{body: "b", count: 2, tags: ["t"]}}], schema)

        assert %{body: "b", count: 2, tags: ["t"]} = decoded.inner
      end
    end
  end

  defp merged(messages, schema) do
    bytes = messages |> Enum.map(&encode(&1, schema)) |> IO.iodata_to_binary()
    Protobuf.decode!("merge.Outer", bytes, schema)
  end

  defp encode(message, schema),
    do: IO.iodata_to_binary(Protobuf.encode!("merge.Outer", message, schema))
end
