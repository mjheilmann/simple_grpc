defmodule SimpleGrpc.Client.DynamicCallTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.{Registry, Schema}
  alias SimpleGrpc.Reflection.Server

  @port 50094
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
      {:ok, %{result: "Total: #{count}", sequence: count, is_final: true}}
    end

    def bidirectional_streaming_call(req_stream, _stream) do
      Stream.map(req_stream, fn req ->
        %{result: "Bidi: " <> req.message, sequence: req.sequence, is_final: false}
      end)
    end
  end

  setup_all do
    {:ok, sup_pid} = Bandit.start_link(plug: SimpleGrpc.Plug, port: @port)

    on_exit(fn ->
      if Process.alive?(sup_pid), do: Process.exit(sup_pid, :shutdown)
    end)

    {:ok, schema} = Schema.from_file(@proto_file)
    %{schema: schema}
  end

  setup do
    Registry.clear()
    SimpleGrpc.register_service(TestServer)
    Server.register()
    :ok
  end

  describe "Dynamic Client.call with reflection: true (in-memory Plug)" do
    test "connects with reflection and invokes unary RPC" do
      {:ok, client} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug, reflection: true)
      assert client.schema != nil

      assert {:ok, response} =
               SimpleGrpc.Client.call(
                 client,
                 "streaming.StreamingService/UnaryCall",
                 %{message: "DynamicHi", sequence: 42}
               )

      assert response.result == "Echo: DynamicHi"
      assert response.sequence == 43
      assert response.is_final == true
    end

    test "invokes unary RPC using call! raising on error" do
      {:ok, client} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug, reflection: true)

      response =
        SimpleGrpc.Client.call!(
          client,
          "streaming.StreamingService/UnaryCall",
          %{message: "BangCall", sequence: 100}
        )

      assert response.result == "Echo: BangCall"
      assert response.sequence == 101
    end

    test "invokes server streaming RPC returning lazy decoded stream" do
      {:ok, client} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug, reflection: true)

      assert {:ok, stream} =
               SimpleGrpc.Client.call(
                 client,
                 "streaming.StreamingService/ServerStreamingCall",
                 %{message: "StreamReq", sequence: 10}
               )

      results = Enum.to_list(stream)
      assert length(results) == 3
      assert Enum.at(results, 0).result == "Chunk 1: StreamReq"
      assert Enum.at(results, 2).is_final == true
    end

    test "invokes client streaming RPC" do
      {:ok, client} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug, reflection: true)

      req_stream =
        1..4
        |> Stream.map(fn i -> %{message: "Item #{i}", sequence: i} end)

      assert {:ok, response} =
               SimpleGrpc.Client.call(
                 client,
                 "streaming.StreamingService/ClientStreamingCall",
                 req_stream
               )

      assert response.result == "Total: 4"
      assert response.sequence == 4
    end

    test "invokes bidirectional streaming RPC" do
      {:ok, client} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug, reflection: true)

      req_stream =
        1..3
        |> Stream.map(fn i -> %{message: "BidiReq #{i}", sequence: i} end)

      assert {:ok, resp_stream} =
               SimpleGrpc.Client.call(
                 client,
                 "streaming.StreamingService/BidirectionalStreamingCall",
                 req_stream
               )

      responses = Enum.to_list(resp_stream)
      assert length(responses) == 3
      assert Enum.at(responses, 0).result == "Bidi: BidiReq 1"
      assert Enum.at(responses, 2).result == "Bidi: BidiReq 3"
    end
  end

  describe "Dynamic Client.call with on-demand reflection discovery" do
    test "dynamically queries reflection when schema is not pre-loaded" do
      # Notice reflection: false / no schema given at connect time
      {:ok, client} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)
      assert client.schema == nil

      # call/4 discovers schema via reflection on-demand!
      assert {:ok, response} =
               SimpleGrpc.Client.call(
                 client,
                 "/streaming.StreamingService/UnaryCall",
                 %{message: "OnDemandHi", sequence: 5}
               )

      assert response.result == "Echo: OnDemandHi"
      assert response.sequence == 6
    end
  end

  describe "Dynamic Client.call with pre-loaded schema option" do
    test "uses schema passed via opts or connect", %{schema: schema} do
      {:ok, client} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug, schema: schema)
      assert client.schema == schema

      assert {:ok, response} =
               SimpleGrpc.Client.call(
                 client,
                 {"streaming.StreamingService", "UnaryCall"},
                 %{message: "Preloaded", sequence: 1}
               )

      assert response.result == "Echo: Preloaded"
    end
  end

  describe "Dynamic Client.call over HTTP/2 Mint transport with reflection" do
    test "connects over HTTP/2 with reflection: true and makes dynamic unary and streaming calls" do
      {:ok, client} = SimpleGrpc.Client.connect("localhost:#{@port}", adapter: :mint, reflection: true)
      assert client.schema != nil

      # 1. Unary call
      assert {:ok, resp} =
               SimpleGrpc.Client.call(
                 client,
                 "streaming.StreamingService/UnaryCall",
                 %{message: "NetworkHi", sequence: 50}
               )

      assert resp.result == "Echo: NetworkHi"
      assert resp.sequence == 51

      # 2. Server streaming call
      assert {:ok, stream} =
               SimpleGrpc.Client.call(
                 client,
                 "streaming.StreamingService/ServerStreamingCall",
                 %{message: "NetworkStream", sequence: 0}
               )

      chunks = Enum.to_list(stream)
      assert length(chunks) == 3
      assert Enum.at(chunks, 0).result == "Chunk 1: NetworkStream"

      SimpleGrpc.Client.close(client)
    end
  end
end
