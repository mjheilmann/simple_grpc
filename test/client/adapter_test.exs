defmodule SimpleGrpc.Client.AdapterTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.Registry
  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Schema

  @proto_file "priv/protos/streaming/streaming_service.proto"

  defmodule TestServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    def unary_call(req, _stream) do
      {:ok, %{result: "Echo: " <> req.message, sequence: req.sequence + 1, is_final: true}}
    end

    def server_streaming_call(req, _stream) do
      1..3
      |> Stream.map(fn i ->
        %{
          result: "Chunk #{i}: #{req.message}",
          sequence: req.sequence + i,
          is_final: i == 3
        }
      end)
    end

    def client_streaming_call(req_stream, _stream) do
      count = Enum.count(req_stream)
      {:ok, %{result: "Total items: #{count}", sequence: count, is_final: true}}
    end

    def bidirectional_streaming_call(req_stream, _stream) do
      Stream.map(req_stream, fn req ->
        %{result: "Bidi: " <> req.message, sequence: req.sequence, is_final: false}
      end)
    end
  end

  setup_all do
    {:ok, sup_pid} = Bandit.start_link(plug: SimpleGrpc.Plug, port: 0)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup_pid)

    on_exit(fn ->
      if Process.alive?(sup_pid), do: Process.exit(sup_pid, :shutdown)
    end)

    {:ok, schema} = Schema.from_file(@proto_file)

    %{schema: schema, port: port}
  end

  setup do
    Registry.clear()
    SimpleGrpc.register_service(TestServer)
    :ok
  end

  describe "Direct In-Memory Plug Adapter (Zero TCP)" do
    test "connects and sends unary RPC via in-memory Plug", %{schema: schema} do
      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)

      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "DirectPlug", sequence: 10}, schema)

      path = "/streaming.StreamingService/UnaryCall"

      {:ok, resp_bytes, headers, _trailers} = SimpleGrpc.Client.send_unary(conn, path, req_bytes)

      assert is_list(headers)
      {:ok, decoded} = Protobuf.decode("StreamResponse", resp_bytes, schema)
      assert decoded.result == "Echo: DirectPlug"
      assert decoded.sequence == 11
      assert decoded.is_final == true
    end

    test "sends streaming request via in-memory Plug", %{schema: schema} do
      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)

      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "StreamPing", sequence: 100}, schema)

      path = "/streaming.StreamingService/ServerStreamingCall"

      {:ok, resp_stream, _headers, _trailers} =
        SimpleGrpc.Client.send_stream(conn, path, req_bytes)

      items =
        resp_stream
        |> Enum.map(fn chunk ->
          {:ok, decoded} = Protobuf.decode("StreamResponse", chunk, schema)
          decoded
        end)

      assert length(items) == 3
      assert Enum.at(items, 0).result == "Chunk 1: StreamPing"
      assert Enum.at(items, 1).result == "Chunk 2: StreamPing"
      assert Enum.at(items, 2).result == "Chunk 3: StreamPing"
    end
  end

  describe "HTTP/2 Network Mint Adapter" do
    test "connects over HTTP/2 and sends unary RPC", %{schema: schema, port: port} do
      {:ok, conn} = SimpleGrpc.Client.connect("127.0.0.1:#{port}", adapter: :mint)

      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "NetworkMint", sequence: 42}, schema)

      path = "/streaming.StreamingService/UnaryCall"

      {:ok, resp_bytes, _headers, _trailers} = SimpleGrpc.Client.send_unary(conn, path, req_bytes)

      {:ok, decoded} = Protobuf.decode("StreamResponse", resp_bytes, schema)
      assert decoded.result == "Echo: NetworkMint"
      assert decoded.sequence == 43
      assert decoded.is_final == true

      :ok = SimpleGrpc.Client.close(conn)
    end

    test "connects over HTTP/2 and streams response chunks", %{schema: schema, port: port} do
      {:ok, conn} = SimpleGrpc.Client.connect("127.0.0.1:#{port}", adapter: :mint)

      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "MintStream", sequence: 200}, schema)

      path = "/streaming.StreamingService/ServerStreamingCall"

      {:ok, resp_stream, _headers, _trailers} =
        SimpleGrpc.Client.send_stream(conn, path, req_bytes)

      items =
        resp_stream
        |> Enum.map(fn chunk ->
          {:ok, decoded} = Protobuf.decode("StreamResponse", chunk, schema)
          decoded
        end)

      assert length(items) == 3
      assert Enum.at(items, 0).result == "Chunk 1: MintStream"
      assert Enum.at(items, 1).result == "Chunk 2: MintStream"
      assert Enum.at(items, 2).result == "Chunk 3: MintStream"

      :ok = SimpleGrpc.Client.close(conn)
    end

    test "streams client request chunks over HTTP/2", %{schema: schema, port: port} do
      {:ok, conn} = SimpleGrpc.Client.connect("127.0.0.1:#{port}", adapter: :mint)

      reqs = [
        Protobuf.encode!("StreamRequest", %{message: "Chunk1", sequence: 1}, schema),
        Protobuf.encode!("StreamRequest", %{message: "Chunk2", sequence: 2}, schema),
        Protobuf.encode!("StreamRequest", %{message: "Chunk3", sequence: 3}, schema)
      ]

      path = "/streaming.StreamingService/ClientStreamingCall"

      {:ok, resp_stream, _headers, _trailers} = SimpleGrpc.Client.send_stream(conn, path, reqs)

      chunks = Enum.to_list(resp_stream)
      assert length(chunks) == 1
      {:ok, decoded} = Protobuf.decode("StreamResponse", hd(chunks), schema)
      assert decoded.result == "Total items: 3"
      assert decoded.sequence == 3

      :ok = SimpleGrpc.Client.close(conn)
    end

    test "streams large unary payloads exceeding HTTP/2 initial window size (> 65,535 bytes)",
         %{schema: schema, port: port} do
      {:ok, conn} = SimpleGrpc.Client.connect("127.0.0.1:#{port}", adapter: :mint)

      large_message = String.duplicate("A", 150_000)

      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: large_message, sequence: 1}, schema)

      assert byte_size(req_bytes) > 65_535

      path = "/streaming.StreamingService/UnaryCall"

      {:ok, resp_bytes, _headers, _trailers} =
        SimpleGrpc.Client.send_unary(conn, path, req_bytes, timeout: 15_000)

      {:ok, decoded} = Protobuf.decode("StreamResponse", resp_bytes, schema)
      assert decoded.result == "Echo: " <> large_message
      assert decoded.sequence == 2

      :ok = SimpleGrpc.Client.close(conn)
    end
  end
end
