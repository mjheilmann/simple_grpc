defmodule SimpleGrpc.TelemetryTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.{Registry, Schema}
  alias SimpleGrpc.Plug, as: GrpcPlug

  @proto_file "priv/protos/streaming/streaming_service.proto"

  defmodule TelemetryServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    def unary_call(%{message: "crash"}, _stream) do
      raise RuntimeError, "Intentional crash for telemetry"
    end

    def unary_call(%{message: "error"}, _stream) do
      {:error, 5, "Resource not found"}
    end

    def unary_call(%{message: msg, sequence: seq}, _stream) do
      {:ok, %{result: "Echo: " <> msg, sequence: seq + 1, is_final: true}}
    end

    def server_streaming_call(r, _), do: r
    def client_streaming_call(r, _), do: r
    def bidirectional_streaming_call(r, _), do: r
  end

  def handle_telemetry_event(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end

  setup do
    Registry.clear()
    SimpleGrpc.register_service(TelemetryServer)

    # Clean up telemetry handlers attached in tests
    test_pid = self()

    on_exit(fn ->
      :telemetry.detach("test-telemetry-start")
      :telemetry.detach("test-telemetry-stop")
      :telemetry.detach("test-telemetry-exception")
    end)

    %{test_pid: test_pid}
  end

  test "emits [:simple_grpc, :call, :start] and [:simple_grpc, :call, :stop] on success", %{
    test_pid: test_pid
  } do
    :telemetry.attach(
      "test-telemetry-start",
      [:simple_grpc, :call, :start],
      &__MODULE__.handle_telemetry_event/4,
      test_pid
    )

    :telemetry.attach(
      "test-telemetry-stop",
      [:simple_grpc, :call, :stop],
      &__MODULE__.handle_telemetry_event/4,
      test_pid
    )

    {:ok, schema} = Schema.from_file(@proto_file)
    input_msg = Schema.message(schema, "StreamRequest")
    {:ok, payload} = SimpleGrpc.Protobuf.encode(input_msg, %{message: "Telemetry Hello", sequence: 10}, schema)
    framed = GrpcPlug.frame_grpc_message(payload)

    conn =
      Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", framed)
      |> Plug.Conn.put_req_header("content-type", "application/grpc")
      |> Plug.Conn.put_req_header("x-trace-id", "trace-123")
      |> GrpcPlug.call(GrpcPlug.init([]))

    assert conn.status == 200

    assert_receive {:telemetry, [:simple_grpc, :call, :start], measurements_start, metadata_start}
    assert metadata_start.path == "/streaming.StreamingService/UnaryCall"
    assert metadata_start.service == "StreamingService"
    assert metadata_start.service_full_name == "streaming.StreamingService"
    assert metadata_start.rpc == "UnaryCall"
    assert is_integer(measurements_start.system_time)
    assert is_integer(measurements_start.monotonic_time)

    assert_receive {:telemetry, [:simple_grpc, :call, :stop], measurements_stop, metadata_stop}
    assert metadata_stop.path == "/streaming.StreamingService/UnaryCall"
    assert metadata_stop.grpc_status == 0
    assert metadata_stop.request.message == "Telemetry Hello"
    assert metadata_stop.response.result == "Echo: Telemetry Hello"
    assert is_integer(measurements_stop.duration)
    assert measurements_stop.duration >= 0
  end

  test "emits [:simple_grpc, :call, :stop] with error code on handler error", %{
    test_pid: test_pid
  } do
    :telemetry.attach(
      "test-telemetry-stop",
      [:simple_grpc, :call, :stop],
      &__MODULE__.handle_telemetry_event/4,
      test_pid
    )

    {:ok, schema} = Schema.from_file(@proto_file)
    input_msg = Schema.message(schema, "StreamRequest")
    {:ok, payload} = SimpleGrpc.Protobuf.encode(input_msg, %{message: "error", sequence: 1}, schema)
    framed = GrpcPlug.frame_grpc_message(payload)

    conn =
      Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", framed)
      |> Plug.Conn.put_req_header("content-type", "application/grpc")
      |> GrpcPlug.call(GrpcPlug.init([]))

    assert conn.status == 200

    assert_receive {:telemetry, [:simple_grpc, :call, :stop], measurements_stop, metadata_stop}
    assert metadata_stop.path == "/streaming.StreamingService/UnaryCall"
    assert metadata_stop.grpc_status == 5
    assert metadata_stop.grpc_message =~ "Resource not found"
    assert is_integer(measurements_stop.duration)
  end

  test "emits [:simple_grpc, :call, :exception] when handler raises exception", %{
    test_pid: test_pid
  } do
    :telemetry.attach(
      "test-telemetry-exception",
      [:simple_grpc, :call, :exception],
      &__MODULE__.handle_telemetry_event/4,
      test_pid
    )

    {:ok, schema} = Schema.from_file(@proto_file)
    input_msg = Schema.message(schema, "StreamRequest")
    {:ok, payload} = SimpleGrpc.Protobuf.encode(input_msg, %{message: "crash", sequence: 1}, schema)
    framed = GrpcPlug.frame_grpc_message(payload)

    conn =
      Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", framed)
      |> Plug.Conn.put_req_header("content-type", "application/grpc")
      |> GrpcPlug.call(GrpcPlug.init([]))

    assert conn.status == 200

    assert_receive {:telemetry, [:simple_grpc, :call, :exception], measurements_exc, metadata_exc}
    assert metadata_exc.path == "/streaming.StreamingService/UnaryCall"
    assert metadata_exc.kind == :error
    assert %RuntimeError{message: "Intentional crash for telemetry"} = metadata_exc.reason
    assert is_list(metadata_exc.stacktrace)
    assert is_integer(measurements_exc.duration)
  end

  test "emits [:simple_grpc, :call, :stop] on UNIMPLEMENTED path", %{test_pid: test_pid} do
    :telemetry.attach(
      "test-telemetry-stop",
      [:simple_grpc, :call, :stop],
      &__MODULE__.handle_telemetry_event/4,
      test_pid
    )

    conn =
      Plug.Test.conn(:post, "/nonexistent.Service/Method", <<>>)
      |> Plug.Conn.put_req_header("content-type", "application/grpc")
      |> GrpcPlug.call(GrpcPlug.init([]))

    assert conn.status == 200

    assert_receive {:telemetry, [:simple_grpc, :call, :stop], measurements_stop, metadata_stop}
    assert metadata_stop.path == "/nonexistent.Service/Method"
    assert metadata_stop.grpc_status == 12
    assert metadata_stop.grpc_message =~ "unknown service or method"
    assert is_integer(measurements_stop.duration)
  end
end
