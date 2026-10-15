defmodule SimpleGrpc.Client.TelemetryAndHeadersTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.Registry

  defmodule HeaderEchoServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/streaming/streaming_service.proto",
      service: "StreamingService"

    defp get_headers(conn_or_stream) do
      case conn_or_stream do
        %Plug.Conn{req_headers: h} -> h
        %{conn: %Plug.Conn{req_headers: h}} -> h
        _ -> []
      end
    end

    def unary_call(req, conn_or_stream) do
      headers = get_headers(conn_or_stream)

      auth = Enum.find_value(headers, "none", fn {k, v} -> if k == "authorization", do: v end)
      trace = Enum.find_value(headers, "none", fn {k, v} -> if k == "x-trace-id", do: v end)
      timeout = Enum.find_value(headers, "none", fn {k, v} -> if k == "grpc-timeout", do: v end)

      case req.message do
        "crash" ->
          raise RuntimeError, "Intentional crash for client telemetry"

        "error" ->
          {:error, 5, "Requested item was not found"}

        _ ->
          {:ok,
           %{
             result:
               "Echo: #{req.message} | Auth: #{auth} | Trace: #{trace} | Timeout: #{timeout}",
             sequence: req.sequence,
             is_final: true
           }}
      end
    end

    def server_streaming_call(req, conn_or_stream) do
      headers = get_headers(conn_or_stream)
      trace = Enum.find_value(headers, "none", fn {k, v} -> if k == "x-trace-id", do: v end)

      1..2
      |> Stream.map(fn i ->
        %{
          result: "Chunk #{i}: #{req.message} | Trace: #{trace}",
          sequence: req.sequence + i,
          is_final: i == 2
        }
      end)
    end

    def client_streaming_call(req_stream, conn_or_stream) do
      headers = get_headers(conn_or_stream)
      trace = Enum.find_value(headers, "none", fn {k, v} -> if k == "x-trace-id", do: v end)

      total = Enum.reduce(req_stream, 0, fn item, acc -> acc + item.sequence end)
      {:ok, %{result: "Sum: #{total} | Trace: #{trace}", sequence: total, is_final: true}}
    end

    def bidirectional_streaming_call(req_stream, _stream) do
      Stream.map(req_stream, fn req ->
        %{result: "Bidi: " <> req.message, sequence: req.sequence, is_final: false}
      end)
    end
  end

  defmodule HeaderClient do
    use SimpleGrpc.Client,
      client_file: "test/fixtures/protos/streaming/streaming_service.proto"
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:client_telemetry, event, measurements, metadata})
  end

  setup_all do
    {:ok, sup_pid} = Bandit.start_link(plug: SimpleGrpc.Plug, port: 0, startup_log: false)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup_pid)

    on_exit(fn ->
      if Process.alive?(sup_pid), do: Process.exit(sup_pid, :shutdown)
    end)

    %{port: port}
  end

  setup do
    Registry.clear()
    SimpleGrpc.register_service(HeaderEchoServer)

    test_pid = self()

    on_exit(fn ->
      :telemetry.detach("test-client-start")
      :telemetry.detach("test-client-stop")
      :telemetry.detach("test-client-exception")
    end)

    %{test_pid: test_pid}
  end

  describe "Client Telemetry Events" do
    test "emits [:simple_grpc, :client, :start] and [:simple_grpc, :client, :stop] on successful Unary RPC",
         %{test_pid: test_pid} do
      :telemetry.attach(
        "test-client-start",
        [:simple_grpc, :client, :start],
        &__MODULE__.handle_telemetry/4,
        test_pid
      )

      :telemetry.attach(
        "test-client-stop",
        [:simple_grpc, :client, :stop],
        &__MODULE__.handle_telemetry/4,
        test_pid
      )

      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)

      req = HeaderClient.build_request(:unary_call, message: "TelemetryHi", sequence: 10)
      assert {:ok, resp} = HeaderClient.unary_call(conn, req)
      assert resp.result =~ "Echo: TelemetryHi"

      # 1. Assert start event
      assert_receive {:client_telemetry, [:simple_grpc, :client, :start], start_meas, start_meta}
      assert start_meta.service == "StreamingService"
      assert start_meta.service_full_name == "streaming.StreamingService"
      assert start_meta.rpc == "UnaryCall"
      assert start_meta.path == "/streaming.StreamingService/UnaryCall"
      assert is_integer(start_meas.system_time)
      assert is_integer(start_meas.monotonic_time)

      # 2. Assert stop event
      assert_receive {:client_telemetry, [:simple_grpc, :client, :stop], stop_meas, stop_meta}
      assert stop_meta.service == "StreamingService"
      assert stop_meta.rpc == "UnaryCall"
      assert stop_meta.status == 0
      assert stop_meta.grpc_status == 0
      assert stop_meta.response.result =~ "Echo: TelemetryHi"
      assert is_integer(stop_meas.duration)
      assert stop_meas.duration >= 0
    end

    test "emits [:simple_grpc, :client, :stop] with error code on gRPC error response", %{
      test_pid: test_pid
    } do
      :telemetry.attach(
        "test-client-stop",
        [:simple_grpc, :client, :stop],
        &__MODULE__.handle_telemetry/4,
        test_pid
      )

      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)

      assert {:error, 5, msg} = HeaderClient.unary_call(conn, %{message: "error", sequence: 1})
      assert msg == "Requested item was not found"

      assert_receive {:client_telemetry, [:simple_grpc, :client, :stop], stop_meas, stop_meta}
      assert stop_meta.status == 5
      assert stop_meta.grpc_status == 5
      assert stop_meta.error == "Requested item was not found"
      assert is_integer(stop_meas.duration)
    end

    test "emits telemetry on dynamic SimpleGrpc.Client.call", %{test_pid: test_pid} do
      :telemetry.attach(
        "test-client-start",
        [:simple_grpc, :client, :start],
        &__MODULE__.handle_telemetry/4,
        test_pid
      )

      :telemetry.attach(
        "test-client-stop",
        [:simple_grpc, :client, :stop],
        &__MODULE__.handle_telemetry/4,
        test_pid
      )

      {:ok, conn} =
        SimpleGrpc.Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          schema: HeaderClient.__schema__()
        )

      assert {:ok, resp} =
               SimpleGrpc.Client.call(
                 conn,
                 "/streaming.StreamingService/UnaryCall",
                 %{message: "DynamicTel", sequence: 99}
               )

      assert resp.result =~ "Echo: DynamicTel"

      assert_receive {:client_telemetry, [:simple_grpc, :client, :start], _m1, meta1}
      assert meta1.rpc == "UnaryCall"
      assert meta1.service == "StreamingService"

      assert_receive {:client_telemetry, [:simple_grpc, :client, :stop], _m2, meta2}
      assert meta2.status == 0
      assert meta2.response.result =~ "Echo: DynamicTel"
    end

    test "emits [:simple_grpc, :client, :start] and [:simple_grpc, :client, :stop] on Streaming RPCs",
         %{
           test_pid: test_pid
         } do
      :telemetry.attach(
        "test-client-start",
        [:simple_grpc, :client, :start],
        &__MODULE__.handle_telemetry/4,
        test_pid
      )

      :telemetry.attach(
        "test-client-stop",
        [:simple_grpc, :client, :stop],
        &__MODULE__.handle_telemetry/4,
        test_pid
      )

      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)

      assert {:ok, stream} =
               HeaderClient.server_streaming_call(conn, %{message: "StreamTel", sequence: 1})

      items = Enum.to_list(stream)
      assert length(items) == 2

      assert_receive {:client_telemetry, [:simple_grpc, :client, :start], _m1, meta1}
      assert meta1.rpc == "ServerStreamingCall"

      assert_receive {:client_telemetry, [:simple_grpc, :client, :stop], _m2, meta2}
      assert meta2.rpc == "ServerStreamingCall"
      assert meta2.status == 0
    end

    test "emits [:simple_grpc, :client, :exception] when an unhandled exception occurs", %{
      test_pid: test_pid
    } do
      :telemetry.attach(
        "test-client-exception",
        [:simple_grpc, :client, :exception],
        &__MODULE__.handle_telemetry/4,
        test_pid
      )

      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)

      try do
        SimpleGrpc.Client.DSL.invoke_unary_rpc(
          conn,
          "/streaming.StreamingService/UnaryCall",
          nil,
          nil,
          nil,
          nil,
          nil,
          nil,
          []
        )
      catch
        _, _ -> :ok
      end

      assert_receive {:client_telemetry, [:simple_grpc, :client, :exception], exc_meas, exc_meta}
      assert exc_meta.rpc == "UnaryCall"
      assert is_integer(exc_meas.duration)
    end
  end

  describe "Custom Request Headers & Metadata" do
    test "passes connection-level default headers to server" do
      {:ok, conn} =
        SimpleGrpc.Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          headers: [authorization: "Bearer default-token", x_trace_id: "conn-trace-123"]
        )

      assert {:ok, resp} =
               HeaderClient.unary_call(conn, %{message: "DefaultHeaders", sequence: 1})

      assert resp.result =~ "Auth: Bearer default-token"
      assert resp.result =~ "Trace: conn-trace-123"
    end

    test "call-level headers override and supplement connection default headers" do
      {:ok, conn} =
        SimpleGrpc.Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          headers: [authorization: "Bearer default-token", x_trace_id: "conn-trace"]
        )

      assert {:ok, resp} =
               HeaderClient.unary_call(conn, %{message: "OverrideHeaders", sequence: 2},
                 headers: [
                   authorization: "Bearer overridden-token",
                   x_trace_id: "call-trace-999"
                 ]
               )

      assert resp.result =~ "Auth: Bearer overridden-token"
      assert resp.result =~ "Trace: call-trace-999"
    end

    test "formats :timeout option into grpc-timeout header" do
      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)

      assert {:ok, resp} =
               HeaderClient.unary_call(conn, %{message: "TimeoutHeader", sequence: 3},
                 timeout: 3500
               )

      assert resp.result =~ "Timeout: 3500m"
    end

    test "HTTP/2 Mint transport sends custom headers and timeout over real connection", %{
      port: port
    } do
      {:ok, conn} =
        SimpleGrpc.Client.connect("localhost:#{port}",
          adapter: :mint,
          headers: [{"authorization", "Bearer mint-net-token"}]
        )

      assert {:ok, resp} =
               HeaderClient.unary_call(conn, %{message: "MintHeaders", sequence: 4},
                 headers: [{"x-trace-id", "mint-trace-777"}],
                 timeout: 4000
               )

      assert resp.result =~ "Auth: Bearer mint-net-token"
      assert resp.result =~ "Trace: mint-trace-777"
      assert resp.result =~ "Timeout: 4000m"

      SimpleGrpc.Client.close(conn)
    end
  end
end
