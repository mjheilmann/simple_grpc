defmodule SimpleGrpc.Client.ReflectionTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Reflection.Descriptor
  alias SimpleGrpc.Client.Reflection, as: ClientReflection

  @proto_file "test/fixtures/protos/streaming/streaming_service.proto"

  defmodule TestServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/streaming/streaming_service.proto"

    def unary_call(req, _stream) do
      {:ok, %{result: "Echo: " <> req.message, sequence: req.sequence + 1, is_final: true}}
    end

    def server_streaming_call(req, _stream) do
      Stream.map(1..2, fn i ->
        %{result: "Chunk #{i}: #{req.message}", sequence: req.sequence + i, is_final: i == 2}
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

  @plug_spec {SimpleGrpc.Plug, services: [TestServer], reflection: true}

  setup_all do
    {:ok, sup_pid} =
      Bandit.start_link(
        plug: @plug_spec,
        port: 0,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup_pid)

    on_exit(fn ->
      if Process.alive?(sup_pid), do: Process.exit(sup_pid, :shutdown)
    end)

    %{port: port}
  end

  describe "Descriptor.decode_file_descriptor/1 round-trip" do
    test "decodes binary FileDescriptorProto into Schema struct with services, messages, and rpcs" do
      {:ok, original_schema} = Schema.from_file(@proto_file)
      fdp_bytes = Descriptor.encode_file_descriptor(original_schema)

      assert is_binary(fdp_bytes)
      assert byte_size(fdp_bytes) > 0

      decoded_schema = Descriptor.decode_file_descriptor(fdp_bytes)

      assert decoded_schema.package == "streaming"
      assert decoded_schema.syntax == :proto3

      # Services
      service =
        decoded_schema.services["StreamingService"] ||
          decoded_schema.services["streaming.StreamingService"]

      assert service != nil
      assert Map.has_key?(service.rpcs, "UnaryCall")
      assert Map.has_key?(service.rpcs, "ServerStreamingCall")
      assert Map.has_key?(service.rpcs, "ClientStreamingCall")
      assert Map.has_key?(service.rpcs, "BidirectionalStreamingCall")

      unary_rpc = service.rpcs["UnaryCall"]
      assert unary_rpc.path == "/streaming.StreamingService/UnaryCall"
      assert unary_rpc.input == "streaming.StreamRequest"
      assert unary_rpc.output == "streaming.StreamResponse"
      refute unary_rpc.request_stream
      refute unary_rpc.response_stream

      server_stream_rpc = service.rpcs["ServerStreamingCall"]
      assert server_stream_rpc.response_stream
      refute server_stream_rpc.request_stream

      # Messages
      req_msg =
        decoded_schema.messages["StreamRequest"] ||
          decoded_schema.messages["streaming.StreamRequest"]

      assert req_msg != nil
      assert SimpleGrpc.Schema.Message.field_by_name(req_msg, :message) != nil
      assert SimpleGrpc.Schema.Message.field_by_name(req_msg, :sequence) != nil
    end
  end

  describe "Client.Reflection with in-memory Plug adapter" do
    test "list_services/2 returns all registered services" do
      {:ok, conn} = SimpleGrpc.Client.connect(@plug_spec, adapter: :plug)

      assert {:ok, services} = ClientReflection.list_services(conn)
      assert "streaming.StreamingService" in services
      assert "grpc.reflection.v1.ServerReflection" in services
      assert "grpc.reflection.v1alpha.ServerReflection" in services
    end

    test "file_containing_symbol/3 returns binary descriptor protos" do
      {:ok, conn} = SimpleGrpc.Client.connect(@plug_spec, adapter: :plug)

      assert {:ok, fdp_list} =
               ClientReflection.file_containing_symbol(conn, "streaming.StreamingService")

      assert length(fdp_list) > 0
      assert Enum.all?(fdp_list, &is_binary/1)
    end

    test "file_containing_symbol/3 returns error when symbol not found" do
      {:ok, conn} = SimpleGrpc.Client.connect(@plug_spec, adapter: :plug)

      assert {:error, 5, msg} =
               ClientReflection.file_containing_symbol(conn, "nonexistent.Symbol")

      assert msg =~ "not found"
    end

    test "file_by_filename/3 returns descriptor protos for synthesized and relative filenames" do
      {:ok, conn} = SimpleGrpc.Client.connect(@plug_spec, adapter: :plug)

      # Synthesized filename (<package>.proto)
      assert {:ok, fdp_list1} = ClientReflection.file_by_filename(conn, "streaming.proto")
      assert length(fdp_list1) > 0

      # Relative proto path
      assert {:ok, fdp_list2} =
               ClientReflection.file_by_filename(conn, "streaming/streaming_service.proto")

      assert length(fdp_list2) > 0
    end

    test "fetch_schema/2 fetches full schema excluding reflection services by default" do
      {:ok, conn} = SimpleGrpc.Client.connect(@plug_spec, adapter: :plug)

      assert {:ok, %Schema{} = schema} = ClientReflection.fetch_schema(conn)

      assert Map.has_key?(schema.services, "StreamingService") or
               Map.has_key?(schema.services, "streaming.StreamingService")

      assert Map.has_key?(schema.messages, "StreamRequest") or
               Map.has_key?(schema.messages, "streaming.StreamRequest")

      assert Map.has_key?(schema.messages, "StreamResponse") or
               Map.has_key?(schema.messages, "streaming.StreamResponse")

      # Reflection service excluded by default
      refute Map.has_key?(schema.services, "grpc.reflection.v1.ServerReflection")
    end

    test "fetch_schema/2 for specific service symbol" do
      {:ok, conn} = SimpleGrpc.Client.connect(@plug_spec, adapter: :plug)

      assert {:ok, %Schema{} = schema} =
               ClientReflection.fetch_schema(conn, "streaming.StreamingService")

      assert Map.has_key?(schema.services, "StreamingService") or
               Map.has_key?(schema.services, "streaming.StreamingService")

      assert Map.has_key?(schema.messages, "StreamRequest") or
               Map.has_key?(schema.messages, "streaming.StreamRequest")
    end

    test "fetch_schema/2 with include_reflection: true includes reflection schemas" do
      {:ok, conn} = SimpleGrpc.Client.connect(@plug_spec, adapter: :plug)

      assert {:ok, %Schema{} = schema} =
               ClientReflection.fetch_schema(conn, include_reflection: true)

      assert Map.has_key?(schema.services, "grpc.reflection.v1.ServerReflection")

      assert Map.has_key?(schema.services, "streaming.StreamingService") or
               Map.has_key?(schema.services, "StreamingService")
    end
  end

  describe "Client.Reflection with network HTTP/2 Mint adapter" do
    test "queries reflection over real HTTP/2 connection", %{port: port} do
      {:ok, conn} = SimpleGrpc.Client.connect("localhost:#{port}", adapter: :mint)

      # 1. List services
      assert {:ok, services} = ClientReflection.list_services(conn)
      assert "streaming.StreamingService" in services

      # 2. Fetch schema dynamically over network
      assert {:ok, %Schema{} = schema} = ClientReflection.fetch_schema(conn)

      assert Map.has_key?(schema.services, "StreamingService") or
               Map.has_key?(schema.services, "streaming.StreamingService")

      SimpleGrpc.Client.close(conn)
    end
  end
end
