defmodule SimpleGrpc.ServerMapValuesTest do
  use SimpleGrpc.Case, plug: SimpleGrpc.Plug

  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Registry
  alias SimpleGrpc.WKT

  defmodule MapperServer do
    use SimpleGrpc.Server, server_file: "test/fixtures/protos/maps/service.proto"

    def echo(req, _stream) do
      :persistent_term.put({__MODULE__, :last_request}, req)
      {:ok, req}
    end
  end

  @inner MapperServer.Maps.Inner
  @deep MapperServer.Maps.Deep

  setup do
    Registry.clear()
    SimpleGrpc.register_service(MapperServer)
    :ok
  end

  @attrs %{
    by_name: %{"a" => %{v: "x"}, "b" => %{v: "y"}},
    by_id: %{1 => %{v: "one"}, 2 => %{v: "two"}},
    modes: %{"m" => :FAST, "n" => :SLOW},
    labels: %{"k" => "v"},
    deep: %{"d" => %{inner: %{v: "i"}, more: %{"q" => %{v: "r"}}, list: [%{v: "l"}]}},
    times: %{"t" => %{seconds: 5, nanos: 1}}
  }

  defp interpreted(schema), do: %{schema | compiled_module: nil}

  describe "encoding" do
    test "the compiled codec encodes maps of messages, enums and well-known types" do
      schema = MapperServer.__schema__()
      bytes = Protobuf.encode!("maps.Holder", @attrs, schema)

      assert {:ok, decoded} = Protobuf.decode("maps.Holder", bytes, interpreted(schema))
      assert decoded.by_name["a"].v == "x"
      assert decoded.by_id[2].v == "two"
      assert decoded.modes == %{"m" => :FAST, "n" => :SLOW}
      assert decoded.labels == %{"k" => "v"}
      assert decoded.deep["d"].more["q"].v == "r"
      assert decoded.times["t"].seconds == 5
    end

    test "both codecs write the same bytes (modulo map order)" do
      schema = MapperServer.__schema__()

      compiled = Protobuf.encode!("maps.Holder", @attrs, schema)
      dynamic = Protobuf.encode!("maps.Holder", @attrs, interpreted(schema))

      {:ok, from_compiled} = Protobuf.decode("maps.Holder", compiled, schema)
      {:ok, from_dynamic} = Protobuf.decode("maps.Holder", dynamic, schema)
      assert from_compiled == from_dynamic
    end
  end

  describe "decoding" do
    test "the compiled codec decodes map values into structs" do
      schema = MapperServer.__schema__()
      bytes = Protobuf.encode!("maps.Holder", @attrs, interpreted(schema))

      {:ok, decoded} = Protobuf.decode("maps.Holder", bytes, schema)

      assert %{"a" => %{__struct__: @inner, v: "x"}} = decoded.by_name
      assert %{2 => %{__struct__: @inner, v: "two"}} = decoded.by_id
      assert decoded.modes == %{"m" => :FAST, "n" => :SLOW}

      assert %{"d" => %{__struct__: @deep, inner: %{__struct__: @inner}, list: [%{v: "l"}]}} =
               decoded.deep

      assert %{__struct__: @inner, v: "r"} = decoded.deep["d"].more["q"]
      assert %WKT.Timestamp{seconds: 5, nanos: 1} = decoded.times["t"]
    end

    test "the interpreted codec gives the same result" do
      schema = MapperServer.__schema__()
      bytes = Protobuf.encode!("maps.Holder", @attrs, interpreted(schema))

      {:ok, compiled} = Protobuf.decode("maps.Holder", bytes, schema)
      {:ok, dynamic} = Protobuf.decode("maps.Holder", bytes, interpreted(schema))

      assert dynamic == compiled
    end

    test "an empty map and an unset map decode to %{}" do
      schema = MapperServer.__schema__()
      {:ok, decoded} = Protobuf.decode("maps.Holder", <<>>, schema)

      assert decoded.by_name == %{}
      assert decoded.modes == %{}
    end
  end

  test "maps of messages work over the wire", %{conn: conn} do
    schema = MapperServer.__schema__()
    request = Protobuf.encode!("maps.Holder", @attrs, schema)

    {:ok, resp, _headers, _trailers} = grpc_call(conn, "/maps.Mapper/Echo", request)
    {:ok, reply} = Protobuf.decode("maps.Holder", resp, schema)

    assert %{__struct__: @inner} =
             :persistent_term.get({MapperServer, :last_request}).by_name["a"]

    assert reply.by_name["b"].v == "y"
    assert reply.deep["d"].inner.v == "i"
    assert reply.modes["n"] == :SLOW
  end
end
