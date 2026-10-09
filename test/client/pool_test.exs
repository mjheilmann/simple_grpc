defmodule SimpleGrpc.Client.PoolTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Schema

  @proto_file "priv/protos/streaming/streaming_service.proto"

  defmodule PoolTestServer do
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
    {:ok, sup_pid} =
      Bandit.start_link(
        plug: {SimpleGrpc.Plug, services: [PoolTestServer]},
        port: 0,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup_pid)

    on_exit(fn ->
      if Process.alive?(sup_pid), do: Process.exit(sup_pid, :shutdown)
    end)

    {:ok, schema} = Schema.from_file(@proto_file)

    %{schema: schema, port: port}
  end

  describe "NimblePool-backed Client Connection Pooling" do
    test "connects with custom pool_size and handles concurrent unary requests",
         %{schema: schema, port: port} do
      {:ok, conn} =
        SimpleGrpc.Client.connect("127.0.0.1:#{port}",
          adapter: :mint,
          pool: true,
          pool_size: 4
        )

      assert match?({:pool, _pid}, conn.state)

      path = "/streaming.StreamingService/UnaryCall"

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            req_bytes =
              Protobuf.encode!("StreamRequest", %{message: "PoolReq#{i}", sequence: i}, schema)

            {:ok, resp_bytes, _headers, _trailers} =
              SimpleGrpc.Client.send_unary(conn, path, req_bytes)

            {:ok, decoded} = Protobuf.decode("StreamResponse", resp_bytes, schema)
            decoded
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert length(results) == 20

      Enum.each(results, fn decoded ->
        assert String.starts_with?(decoded.result, "Echo: PoolReq")
        assert decoded.is_final == true
      end)

      :ok = SimpleGrpc.Client.close(conn)
    end

    test "handles server streaming over pooled connections", %{schema: schema, port: port} do
      {:ok, conn} =
        SimpleGrpc.Client.connect("127.0.0.1:#{port}",
          adapter: :mint,
          pool: true,
          pool_size: 2
        )

      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "StreamPool", sequence: 10}, schema)

      path = "/streaming.StreamingService/ServerStreamingCall"

      {:ok, resp_stream, _headers, _trailers} =
        SimpleGrpc.Client.send_stream(conn, path, req_bytes)

      chunks =
        resp_stream
        |> Enum.map(fn chunk ->
          {:ok, decoded} = Protobuf.decode("StreamResponse", chunk, schema)
          decoded
        end)

      assert length(chunks) == 3
      assert Enum.at(chunks, 0).result == "Chunk 1: StreamPool"
      assert Enum.at(chunks, 2).result == "Chunk 3: StreamPool"

      :ok = SimpleGrpc.Client.close(conn)
    end

    test "supports disabling pool with pool: false", %{schema: schema, port: port} do
      {:ok, conn} =
        SimpleGrpc.Client.connect("127.0.0.1:#{port}",
          adapter: :mint,
          pool: false
        )

      assert match?({:conn, _pid}, conn.state)

      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "NoPool", sequence: 99}, schema)

      path = "/streaming.StreamingService/UnaryCall"

      {:ok, resp_bytes, _headers, _trailers} =
        SimpleGrpc.Client.send_unary(conn, path, req_bytes)

      {:ok, decoded} = Protobuf.decode("StreamResponse", resp_bytes, schema)
      assert decoded.result == "Echo: NoPool"
      assert decoded.sequence == 100

      :ok = SimpleGrpc.Client.close(conn)
    end

    test "replaces dead workers in pool automatically", %{schema: schema, port: port} do
      {:ok, conn} =
        SimpleGrpc.Client.connect("127.0.0.1:#{port}",
          adapter: :mint,
          pool: true,
          pool_size: 2
        )

      {:pool, pool_pid} = conn.state

      # Check out a worker and forcefully terminate its process
      SimpleGrpc.Client.Pool.checkout(pool_pid, fn worker_pid ->
        Process.exit(worker_pid, :kill)
      end)

      # Wait a moment for process exit to register
      Process.sleep(50)

      # Subsequent calls should succeed by initializing a replacement worker
      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "AfterKill", sequence: 1}, schema)

      path = "/streaming.StreamingService/UnaryCall"

      {:ok, resp_bytes, _headers, _trailers} =
        SimpleGrpc.Client.send_unary(conn, path, req_bytes)

      {:ok, decoded} = Protobuf.decode("StreamResponse", resp_bytes, schema)
      assert decoded.result == "Echo: AfterKill"

      :ok = SimpleGrpc.Client.close(conn)
    end
  end
end
