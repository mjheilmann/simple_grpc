defmodule SimpleGrpc.GrpcurlTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.{Registry, Schema}

  @port 50099
  @proto_file "priv/protos/streaming/streaming_service.proto"

  defmodule StreamingServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    def unary_call(%{message: msg, sequence: seq}, _stream) do
      %{result: "Processed: " <> msg, sequence: seq + 1, is_final: true}
    end

    def server_streaming_call(req, _stream), do: req
    def client_streaming_call(req, _stream), do: req
    def bidirectional_streaming_call(req, _stream), do: req
  end

  setup_all do
    grpcurl_path = System.find_executable("grpcurl") || "/opt/homebrew/bin/grpcurl"

    if not File.exists?(grpcurl_path) do
      raise "grpcurl executable not found"
    end

    {:ok, sup_pid} = Bandit.start_link(plug: SimpleGrpc.Plug, port: @port)

    on_exit(fn ->
      if Process.alive?(sup_pid) do
        Process.exit(sup_pid, :shutdown)
      end
    end)

    %{grpcurl: grpcurl_path}
  end

  setup do
    # Reset registry to static StreamingServer before each test
    Registry.clear()
    SimpleGrpc.register_service(StreamingServer)
    :ok
  end

  test "grpcurl calls static SimpleGrpc.Server module over HTTP/2", %{grpcurl: grpcurl} do
    {output, 0} =
      System.cmd(grpcurl, [
        "-plaintext",
        "-proto", @proto_file,
        "-d", ~s({"message": "Hello from grpcurl!", "sequence": 10}),
        "127.0.0.1:#{@port}",
        "streaming.StreamingService/UnaryCall"
      ])

    assert output =~ ~s("result": "Processed: Hello from grpcurl!")
    assert output =~ ~s("sequence": 11)
    assert output =~ ~s("isFinal": true)
  end

  test "grpcurl calls dynamically registered service and supports runtime hot-replacement", %{grpcurl: grpcurl} do
    # 1. Register a new service dynamically at runtime
    schema = Schema.from_file!(@proto_file)

    SimpleGrpc.register_service(schema, %{
      unary_call: fn %{message: m, sequence: s} ->
        %{result: "dynamic_v1: " <> m, sequence: s * 2, is_final: true}
      end
    })

    {output1, 0} =
      System.cmd(grpcurl, [
        "-plaintext",
        "-proto", @proto_file,
        "-d", ~s({"message": "Runtime Test", "sequence": 5}),
        "127.0.0.1:#{@port}",
        "streaming.StreamingService/UnaryCall"
      ])

    assert output1 =~ ~s("result": "dynamic_v1: Runtime Test")
    assert output1 =~ ~s("sequence": 10)

    # 2. Hot-swap the handler at runtime
    path = "/streaming.StreamingService/UnaryCall"
    {:ok, route} = Registry.lookup_route(path)

    Registry.register_rpc(path, %{
      route
      | handler: fn %{message: m, sequence: s} ->
          %{result: "dynamic_v2: " <> m, sequence: s * 10, is_final: false}
        end
    })

    {output2, 0} =
      System.cmd(grpcurl, [
        "-plaintext",
        "-proto", @proto_file,
        "-d", ~s({"message": "Runtime Test", "sequence": 5}),
        "127.0.0.1:#{@port}",
        "streaming.StreamingService/UnaryCall"
      ])

    assert output2 =~ ~s("result": "dynamic_v2: Runtime Test")
    assert output2 =~ ~s("sequence": 50)
  end
end
