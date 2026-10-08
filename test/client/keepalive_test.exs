defmodule SimpleGrpc.Client.KeepaliveTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.Registry
  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Schema

  @proto_file "priv/protos/streaming/streaming_service.proto"

  defmodule KeepaliveServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    def unary_call(req, _stream) do
      {:ok, %{result: "Echo: " <> req.message, sequence: req.sequence + 1, is_final: true}}
    end

    def server_streaming_call(req, _stream) do
      1..2
      |> Stream.map(fn i ->
        %{
          result: "Chunk #{i}: #{req.message}",
          sequence: req.sequence + i,
          is_final: i == 2
        }
      end)
    end

    def client_streaming_call(req_stream, _stream) do
      count = Enum.count(req_stream)
      {:ok, %{result: "Total: #{count}", sequence: count, is_final: true}}
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
    SimpleGrpc.register_service(KeepaliveServer)
    :ok
  end

  describe "HTTP/2 Keepalive & Ping Probes" do
    test "sends manual HTTP/2 ping and returns round-trip latency on pooled Mint connection",
         %{port: port} do
      {:ok, conn} =
        SimpleGrpc.Client.connect("127.0.0.1:#{port}",
          adapter: :mint,
          pool: true,
          pool_size: 2
        )

      assert {:ok, latency_us} = SimpleGrpc.Client.ping(conn)
      assert is_integer(latency_us)
      assert latency_us >= 0

      # Ping with custom payload
      custom_payload = <<1, 2, 3, 4, 5, 6, 7, 8>>
      assert {:ok, latency_us2} = SimpleGrpc.Client.ping(conn, payload: custom_payload)
      assert is_integer(latency_us2)

      :ok = SimpleGrpc.Client.close(conn)
    end

    test "sends manual HTTP/2 ping on single (unpooled) Mint connection", %{port: port} do
      {:ok, conn} =
        SimpleGrpc.Client.connect("127.0.0.1:#{port}",
          adapter: :mint,
          pool: false
        )

      assert {:ok, latency_us} = SimpleGrpc.Client.ping(conn)
      assert is_integer(latency_us)
      assert latency_us >= 0

      :ok = SimpleGrpc.Client.close(conn)
    end

    test "ping on in-memory Plug adapter returns {:ok, 0}", %{} do
      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)
      assert {:ok, 0} = SimpleGrpc.Client.ping(conn)
      :ok = SimpleGrpc.Client.close(conn)
    end

    test "automatic background keepalive pings keep connection alive over idle periods",
         %{schema: schema, port: port} do
      {:ok, conn} =
        SimpleGrpc.Client.connect("127.0.0.1:#{port}",
          adapter: :mint,
          pool: true,
          pool_size: 1,
          keepalive_time: 100,
          keepalive_timeout: 1000,
          permit_without_calls: true
        )

      # Sleep across multiple keepalive tick intervals
      Process.sleep(350)

      # Ensure subsequent RPC executes cleanly without connection drops
      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "Alive", sequence: 1}, schema)

      path = "/streaming.StreamingService/UnaryCall"

      {:ok, resp_bytes, _headers, _trailers} =
        SimpleGrpc.Client.send_unary(conn, path, req_bytes)

      {:ok, decoded} = Protobuf.decode("StreamResponse", resp_bytes, schema)
      assert decoded.result == "Echo: Alive"

      :ok = SimpleGrpc.Client.close(conn)
    end
  end
end
