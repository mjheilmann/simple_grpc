defmodule SimpleGrpc.ServerDecodeStructsTest do
  use SimpleGrpc.Case, plug: SimpleGrpc.Plug

  import Bitwise

  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Registry

  defmodule EchoServer do
    use SimpleGrpc.Server, server_file: "test/fixtures/protos/structs/service.proto"

    def echo(req, _stream) do
      :persistent_term.put({__MODULE__, :last_request}, req)
      {:ok, %{id: req.id, inner: req.inner, items: req.items}}
    end
  end

  defmodule SwapperServer do
    use SimpleGrpc.Server,
      files: ["test/fixtures/protos/multi_file/same_name/*.proto"]

    def swap(req, _stream) do
      :persistent_term.put({__MODULE__, :last_request}, req)
      {:ok, %{b: req.a}}
    end

    def wrap(req, _stream) do
      :persistent_term.put({__MODULE__, :last_request}, req)
      {:ok, %{item: %{b: req.item.a}, items: Enum.map(req.items, &%{b: &1.a})}}
    end

    def nest(req, _stream) do
      :persistent_term.put({__MODULE__, :last_request}, req)
      {:ok, req}
    end
  end

  defmodule RelativeServer do
    use SimpleGrpc.Server, files: ["test/fixtures/protos/multi_file/rel_rpc/*.proto"]

    def run(req, _stream) do
      :persistent_term.put({__MODULE__, :last_request}, req)
      {:ok, %{a: req.a}}
    end

    def wrap(req, _stream) do
      :persistent_term.put({__MODULE__, :last_request}, req)
      {:ok, %{item: %{b: req.item.a}}}
    end
  end

  @envelope EchoServer.Structs.Envelope
  @inner EchoServer.Structs.Inner

  setup do
    Registry.clear()
    SimpleGrpc.register_service(EchoServer)
    SimpleGrpc.register_service(SwapperServer)
    SimpleGrpc.register_service(RelativeServer)
    :ok
  end

  defp echo(conn, attrs, extra_wire \\ <<>>) do
    schema = EchoServer.__schema__()
    request = encode_request(attrs) <> extra_wire
    {:ok, resp, _headers, _trailers} = grpc_call(conn, "/structs.Echoer/Echo", request)
    {:ok, reply} = Protobuf.decode("structs.Envelope", resp, schema)
    {:persistent_term.get({EchoServer, :last_request}), reply}
  end

  defp encode_request(attrs) do
    Protobuf.encode!("structs.Envelope", attrs, EchoServer.__schema__())
  end

  describe "the handler receives structs" do
    test "the request is the server's struct for the message", %{conn: conn} do
      {req, _reply} = echo(conn, %{id: "a"})

      assert %{__struct__: @envelope, id: "a"} = req
    end

    test "nested, repeated and oneof messages are structs too", %{conn: conn} do
      {req, _reply} =
        echo(conn, %{
          inner: %{v: "x"},
          items: [%{v: "y"}, %{v: "z"}],
          chosen: %{v: "c"}
        })

      assert %{__struct__: @inner, v: "x"} = req.inner
      assert [%{__struct__: @inner, v: "y"}, %{__struct__: @inner, v: "z"}] = req.items
      assert %{__struct__: @inner, v: "c"} = req.chosen
    end

    test "enums stay atoms", %{conn: conn} do
      {req, _reply} = echo(conn, %{mode: :FAST})

      assert req.mode == :FAST
    end

    test "unset fields keep their proto3 defaults", %{conn: conn} do
      {req, _reply} = echo(conn, %{})

      assert req.id == ""
      assert req.inner == nil
      assert req.items == []
      assert req.mode == :MODE_UNSPECIFIED
      assert req.__unknown_fields__ == []
    end

    test "fields the server does not know are kept in __unknown_fields__", %{conn: conn} do
      # field 99, varint 5
      unknown = <<99 <<< 3 ||| 0, 5>>
      {req, _reply} = echo(conn, %{id: "u"}, unknown)

      assert req.id == "u"
      assert req.__unknown_fields__ == [unknown]
    end

    test "a struct response round-trips", %{conn: conn} do
      {_req, reply} = echo(conn, %{id: "r", inner: %{v: "x"}, items: [%{v: "y"}]})

      assert reply.id == "r"
      assert reply.inner.v == "x"
      assert [%{v: "y"}] = reply.items
    end
  end

  describe "unknown fields" do
    test "are written back when the struct is encoded" do
      schema = EchoServer.__schema__()
      unknown = <<99 <<< 3 ||| 0, 5>>

      bytes =
        Protobuf.encode!(
          "structs.Inner",
          struct(@inner, v: "a", __unknown_fields__: [unknown]),
          schema
        )

      {:ok, decoded} = Protobuf.decode("structs.Inner", bytes, schema)

      assert decoded.v == "a"
      assert decoded.__unknown_fields__ == [unknown]
    end
  end

  describe "the interpreted codec matches the compiled one" do
    test "decoding produces the same structs" do
      schema = EchoServer.__schema__()
      assert schema.compiled_module
      interpreted = %{schema | compiled_module: nil}

      bytes =
        encode_request(%{
          id: "i",
          inner: %{v: "x"},
          items: [%{v: "y"}],
          chosen: %{v: "c"},
          mode: :FAST
        })

      {:ok, compiled} = Protobuf.decode("structs.Envelope", bytes, schema)
      {:ok, dynamic} = Protobuf.decode("structs.Envelope", bytes, interpreted)

      assert %{__struct__: @envelope} = dynamic
      assert dynamic == compiled
    end
  end

  describe "messages with the same short name in different packages" do
    test "each decodes to its own package's struct", %{conn: conn} do
      schema = SwapperServer.__schema__()
      request = Protobuf.encode!("same_one.Item", %{a: "hello"}, schema)

      {:ok, resp, _headers, _trailers} = grpc_call(conn, "/same_svc.Swapper/Swap", request)
      {:ok, reply} = Protobuf.decode("same_two.Item", resp, schema)

      assert %{__struct__: SwapperServer.SameOne.Item, a: "hello"} =
               :persistent_term.get({SwapperServer, :last_request})

      assert %{__struct__: SwapperServer.SameTwo.Item, b: "hello"} = reply
    end
  end

  describe "relative type names resolve from the scope that declares them" do
    test "a field type resolves within its own package", %{conn: conn} do
      schema = SwapperServer.__schema__()

      request =
        Protobuf.encode!(
          "same_one.Box",
          %{item: %{a: "one"}, items: [%{a: "x"}, %{a: "y"}]},
          schema
        )

      {:ok, resp, _headers, _trailers} = grpc_call(conn, "/same_svc.Swapper/Wrap", request)
      {:ok, reply} = Protobuf.decode("same_two.Box", resp, schema)

      assert %{
               __struct__: SwapperServer.SameOne.Box,
               item: %{__struct__: SwapperServer.SameOne.Item, a: "one"},
               items: [%{__struct__: SwapperServer.SameOne.Item}, %{a: "y"}]
             } = :persistent_term.get({SwapperServer, :last_request})

      assert %{
               __struct__: SwapperServer.SameTwo.Box,
               item: %{__struct__: SwapperServer.SameTwo.Item, b: "one"},
               items: [%{__struct__: SwapperServer.SameTwo.Item, b: "x"}, %{b: "y"}]
             } = reply
    end

    test "a nested message type wins over a top-level one of the same name", %{conn: conn} do
      schema = SwapperServer.__schema__()
      request = Protobuf.encode!("same_one.Outer", %{inner: %{n: "deep"}}, schema)

      {:ok, resp, _headers, _trailers} = grpc_call(conn, "/same_svc.Swapper/Nest", request)
      {:ok, reply} = Protobuf.decode("same_one.Outer", resp, schema)

      assert %{inner: %{__struct__: SwapperServer.SameOne.Outer.Item, n: "deep"}} =
               :persistent_term.get({SwapperServer, :last_request})

      assert %{inner: %{__struct__: SwapperServer.SameOne.Outer.Item, n: "deep"}} = reply
    end

    test "an rpc type written relatively resolves in the service's package", %{conn: conn} do
      schema = RelativeServer.__schema__()
      request = Protobuf.encode!("rel_a.Item", %{a: "r"}, schema)

      {:ok, resp, _headers, _trailers} = grpc_call(conn, "/rel_a.Relative/Run", request)
      {:ok, reply} = Protobuf.decode("rel_a.Item", resp, schema)

      assert %{__struct__: RelativeServer.RelA.Item, a: "r"} =
               :persistent_term.get({RelativeServer, :last_request})

      assert reply.a == "r"
    end
  end

  describe "relative type names in the interpreted codec" do
    test "resolve the same way as in the compiled one" do
      schema = SwapperServer.__schema__()
      interpreted = %{schema | compiled_module: nil}

      bytes = Protobuf.encode!("same_one.Box", %{item: %{a: "one"}, items: [%{a: "x"}]}, schema)

      {:ok, compiled} = Protobuf.decode("same_one.Box", bytes, schema)
      {:ok, dynamic} = Protobuf.decode("same_one.Box", bytes, interpreted)

      assert %{item: %{__struct__: SwapperServer.SameOne.Item}} = dynamic
      assert dynamic == compiled
    end
  end

  describe "Message.new/1" do
    test "accepts a decoded struct" do
      struct = struct(@envelope, id: "n", __unknown_fields__: [<<1>>])

      assert %{__struct__: @envelope, id: "n", __unknown_fields__: [<<1>>]} =
               @envelope.new(struct)
    end
  end
end
