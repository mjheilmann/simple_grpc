defmodule SimpleGrpc.ClientDSLTest do
  use ExUnit.Case, async: true

  # Define typed Client using SimpleGrpc.Client
  defmodule StreamingClient do
    use SimpleGrpc.Client,
      client_file: "test/fixtures/protos/streaming/streaming_service.proto"
  end

  # Define Server
  defmodule StreamingServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/streaming/streaming_service.proto",
      service: "StreamingService"

    def unary_call(req, _stream) do
      {:ok, %{result: "Echo: " <> req.message, sequence: req.sequence + 1, is_final: true}}
    end

    def server_streaming_call(req, _stream) do
      1..3
      |> Stream.map(fn i ->
        %{result: "Chunk #{i}: #{req.message}", sequence: req.sequence + i, is_final: i == 3}
      end)
    end

    def client_streaming_call(req_stream, _stream) do
      count = Enum.count(req_stream)
      {:ok, %{result: "Aggregated #{count} chunks", sequence: count, is_final: true}}
    end

    def bidirectional_streaming_call(req_stream, _stream) do
      Stream.map(req_stream, fn req ->
        %{result: "Bidi: " <> req.message, sequence: req.sequence * 2, is_final: false}
      end)
    end
  end

  setup_all do
    {:ok, sup_pid} =
      Bandit.start_link(
        plug: {SimpleGrpc.Plug, services: [StreamingServer]},
        port: 0,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup_pid)

    on_exit(fn ->
      if Process.alive?(sup_pid), do: Process.exit(sup_pid, :shutdown)
    end)

    %{port: port}
  end

  describe "Compile-Time Client Generation & Helpers" do
    test "generates nested message structs and metadata" do
      req = StreamingClient.StreamRequest.new(message: "StructTest", sequence: 99)
      assert req == %StreamingClient.StreamRequest{message: "StructTest", sequence: 99}

      resp =
        StreamingClient.build_response(:unary_call,
          result: "BuiltResp",
          sequence: 100,
          is_final: true
        )

      assert resp == %StreamingClient.StreamResponse{
               result: "BuiltResp",
               sequence: 100,
               is_final: true
             }

      assert StreamingClient.__service_name__() == "StreamingService"
      assert StreamingClient.grpc_path(:unary_call) == "/streaming.StreamingService/UnaryCall"
    end
  end

  describe "In-Memory Plug Transport with Typed Client" do
    test "Unary RPC call & bang variant" do
      {:ok, conn} =
        SimpleGrpc.Client.connect({SimpleGrpc.Plug, services: [StreamingServer]}, adapter: :plug)

      # 1. Map/keyword argument
      {:ok, resp} = StreamingClient.unary_call(conn, %{message: "HelloPlug", sequence: 10})

      assert resp == %StreamingClient.StreamResponse{
               result: "Echo: HelloPlug",
               sequence: 11,
               is_final: true
             }

      # 2. Bang call
      resp_bang = StreamingClient.unary_call!(conn, message: "HelloBang", sequence: 20)

      assert resp_bang == %StreamingClient.StreamResponse{
               result: "Echo: HelloBang",
               sequence: 21,
               is_final: true
             }
    end

    test "Server Streaming RPC returns typed struct stream" do
      {:ok, conn} =
        SimpleGrpc.Client.connect({SimpleGrpc.Plug, services: [StreamingServer]}, adapter: :plug)

      {:ok, resp_stream} =
        StreamingClient.server_streaming_call(conn, %{message: "StreamPing", sequence: 100})

      items = Enum.to_list(resp_stream)
      assert length(items) == 3

      assert Enum.at(items, 0) == %StreamingClient.StreamResponse{
               result: "Chunk 1: StreamPing",
               sequence: 101,
               is_final: false
             }

      assert Enum.at(items, 2) == %StreamingClient.StreamResponse{
               result: "Chunk 3: StreamPing",
               sequence: 103,
               is_final: true
             }
    end

    test "Client Streaming RPC aggregates stream of inputs" do
      {:ok, conn} =
        SimpleGrpc.Client.connect({SimpleGrpc.Plug, services: [StreamingServer]}, adapter: :plug)

      requests = [
        %{message: "A", sequence: 1},
        %{message: "B", sequence: 2},
        %{message: "C", sequence: 3}
      ]

      {:ok, resp} = StreamingClient.client_streaming_call(conn, requests)

      assert resp == %StreamingClient.StreamResponse{
               result: "Aggregated 3 chunks",
               sequence: 3,
               is_final: true
             }
    end

    test "Bidirectional Streaming RPC" do
      {:ok, conn} =
        SimpleGrpc.Client.connect({SimpleGrpc.Plug, services: [StreamingServer]}, adapter: :plug)

      requests = [
        %{message: "Alpha", sequence: 5},
        %{message: "Beta", sequence: 10}
      ]

      {:ok, resp_stream} = StreamingClient.bidirectional_streaming_call(conn, requests)

      items = Enum.to_list(resp_stream)
      assert length(items) == 2

      assert Enum.at(items, 0) == %StreamingClient.StreamResponse{
               result: "Bidi: Alpha",
               sequence: 10,
               is_final: false
             }

      assert Enum.at(items, 1) == %StreamingClient.StreamResponse{
               result: "Bidi: Beta",
               sequence: 20,
               is_final: false
             }
    end
  end

  describe "HTTP/2 Network Mint Transport with Typed Client" do
    test "Unary RPC over HTTP/2", %{port: port} do
      {:ok, conn} = SimpleGrpc.Client.connect("127.0.0.1:#{port}", adapter: :mint)

      {:ok, resp} = StreamingClient.unary_call(conn, %{message: "MintClient", sequence: 50})
      assert resp.result == "Echo: MintClient"
      assert resp.sequence == 51

      :ok = SimpleGrpc.Client.close(conn)
    end

    test "Server Streaming RPC over HTTP/2", %{port: port} do
      {:ok, conn} = SimpleGrpc.Client.connect("127.0.0.1:#{port}", adapter: :mint)

      {:ok, resp_stream} =
        StreamingClient.server_streaming_call(conn, %{message: "MintStream", sequence: 200})

      items = Enum.to_list(resp_stream)
      assert length(items) == 3
      assert Enum.at(items, 0).result == "Chunk 1: MintStream"
      assert Enum.at(items, 1).result == "Chunk 2: MintStream"
      assert Enum.at(items, 2).result == "Chunk 3: MintStream"

      :ok = SimpleGrpc.Client.close(conn)
    end
  end
end
