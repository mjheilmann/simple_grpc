defmodule SimpleGrpc.ServerWellKnownTypesTest do
  use SimpleGrpc.Case, plug: SimpleGrpc.Plug

  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Registry

  defmodule StamperServer do
    use SimpleGrpc.Server, server_file: "test/fixtures/protos/wkt/service.proto"

    def stamp(req, _stream) do
      :persistent_term.put({__MODULE__, :last_request}, req)
      {:ok, req}
    end
  end

  defmodule FailerServer do
    use SimpleGrpc.Server, server_file: "test/fixtures/protos/wkt/transitive.proto"

    def fail(req, _stream), do: {:ok, req}
  end

  defmodule WindowerServer do
    use SimpleGrpc.Server, server_file: "test/fixtures/protos/wkt/own_duration.proto"

    def window(req, _stream), do: {:ok, req}
  end

  setup do
    Registry.clear()
    SimpleGrpc.register_service(StamperServer)
    :ok
  end

  @attrs %{
    id: "s",
    at: %{seconds: 1_700_000_000, nanos: 5},
    ttl: %{seconds: 60, nanos: 0},
    mask: %{paths: ["a", "b.c"]},
    history: [%{seconds: 1, nanos: 0}, %{seconds: 2, nanos: 0}]
  }

  test "the schema contains the well-known types the files import" do
    messages = StamperServer.__schema__().messages

    for name <- ~w(Timestamp Duration FieldMask) do
      assert Map.has_key?(messages, "google.protobuf.#{name}")
    end
  end

  test "well-known type fields round-trip over the wire", %{conn: conn} do
    schema = StamperServer.__schema__()
    request = Protobuf.encode!("wkt.Stamped", @attrs, schema)

    {:ok, resp, _headers, _trailers} = grpc_call(conn, "/wkt.Stamper/Stamp", request)
    {:ok, reply} = Protobuf.decode("wkt.Stamped", resp, schema)

    assert reply.at == %{seconds: 1_700_000_000, nanos: 5}
    assert reply.ttl == %{seconds: 60, nanos: 0}
    assert reply.mask == %{paths: ["a", "b.c"]}
    assert reply.history == [%{seconds: 1, nanos: 0}, %{seconds: 2, nanos: 0}]
  end

  test "the handler gets the struct with well-known types as plain maps", %{conn: conn} do
    schema = StamperServer.__schema__()
    request = Protobuf.encode!("wkt.Stamped", @attrs, schema)
    {:ok, _resp, _headers, _trailers} = grpc_call(conn, "/wkt.Stamper/Stamp", request)

    req = :persistent_term.get({StamperServer, :last_request})

    assert %{__struct__: StamperServer.Wkt.Stamped, id: "s"} = req
    assert req.at == %{seconds: 1_700_000_000, nanos: 5}
    refute is_struct(req.at)
  end

  test "the interpreted codec agrees" do
    schema = StamperServer.__schema__()
    bytes = Protobuf.encode!("wkt.Stamped", @attrs, schema)

    {:ok, compiled} = Protobuf.decode("wkt.Stamped", bytes, schema)
    {:ok, dynamic} = Protobuf.decode("wkt.Stamped", bytes, %{schema | compiled_module: nil})

    assert dynamic == compiled
  end

  describe "imports of imports" do
    test "a well-known type that imports another brings it along" do
      schema = FailerServer.__schema__()
      assert Map.has_key?(schema.messages, "google.protobuf.Any")

      attrs = %{status: %{code: 3, message: "m", details: [%{type_url: "t", value: <<1>>}]}}
      bytes = Protobuf.encode!("wkt_transitive.Failure", attrs, schema)
      {:ok, decoded} = Protobuf.decode("wkt_transitive.Failure", bytes, schema)

      assert decoded.status.code == 3
      assert [%{type_url: "t", value: <<1>>}] = decoded.status.details
    end
  end

  describe "a listed message that shares a short name with a well-known type" do
    test "keeps its own definition next to the well-known one" do
      schema = WindowerServer.__schema__()

      attrs = %{own: %{text: "mine"}, real: %{seconds: 9, nanos: 1}}
      bytes = Protobuf.encode!("wkt_own.Window", attrs, schema)
      {:ok, decoded} = Protobuf.decode("wkt_own.Window", bytes, schema)

      assert %{__struct__: WindowerServer.WktOwn.Duration, text: "mine"} = decoded.own
      assert decoded.real == %{seconds: 9, nanos: 1}
    end
  end
end
