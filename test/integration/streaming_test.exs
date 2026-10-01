defmodule SimpleGrpc.StreamingIntegrationTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.Registry

  @port 50098
  @proto_file "priv/protos/streaming/streaming_service.proto"

  defmodule FullStreamingServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    # 1. Unary
    def unary_call(req, _stream) do
      {:ok, %{result: "Unary: " <> req.message, sequence: req.sequence, is_final: true}}
    end

    # 2. Server Streaming (Single Request -> Stream of Responses)
    def server_streaming_call(req, _stream) do
      1..3
      |> Stream.map(fn i ->
        %{
          result: "ServerChunk #{i} for #{req.message}",
          sequence: req.sequence + i,
          is_final: i == 3
        }
      end)
    end

    # 3. Client Streaming (Stream of Requests -> Single Response)
    def client_streaming_call(req_stream, _stream) do
      {count, messages, last_seq} =
        Enum.reduce(req_stream, {0, [], 0}, fn req, {c, msgs, _} ->
          {c + 1, [req.message | msgs], req.sequence}
        end)

      {:ok,
       %{
         result: "Received #{count} items: " <> Enum.join(Enum.reverse(messages), ", "),
         sequence: last_seq,
         is_final: true
       }}
    end

    # 4. Bidirectional Streaming (Stream of Requests -> Stream of Responses)
    def bidirectional_streaming_call(req_stream, _stream) do
      Stream.map(req_stream, fn req ->
        %{
          result: "BidiEcho: " <> req.message,
          sequence: req.sequence * 10,
          is_final: false
        }
      end)
    end
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
    Registry.clear()
    SimpleGrpc.register_service(FullStreamingServer)
    :ok
  end

  test "Server Streaming: Client sends 1 request, server returns stream of 3 chunks", %{
    grpcurl: grpcurl
  } do
    {output, 0} =
      System.cmd(
        grpcurl,
        [
          "-plaintext",
          "-proto",
          @proto_file,
          "-d",
          ~s({"message": "HelloStream", "sequence": 100}),
          "127.0.0.1:#{@port}",
          "streaming.StreamingService/ServerStreamingCall"
        ],
        stderr_to_stdout: true
      )

    assert output =~ ~s("result": "ServerChunk 1 for HelloStream")
    assert output =~ ~s("sequence": 101)
    assert output =~ ~s("result": "ServerChunk 2 for HelloStream")
    assert output =~ ~s("sequence": 102)
    assert output =~ ~s("result": "ServerChunk 3 for HelloStream")
    assert output =~ ~s("sequence": 103)
    assert output =~ ~s("isFinal": true)
  end

  test "Client Streaming: Client streams multiple JSON frames, server aggregates", %{
    grpcurl: grpcurl
  } do
    frames = ~s({"message": "First", "sequence": 1}\n{"message": "Second", "sequence": 2}\n{"message": "Third", "sequence": 3})

    {output, 0} =
      System.cmd(
        grpcurl,
        [
          "-plaintext",
          "-proto",
          @proto_file,
          "-d",
          frames,
          "127.0.0.1:#{@port}",
          "streaming.StreamingService/ClientStreamingCall"
        ],
        stderr_to_stdout: true
      )

    assert output =~ ~s("result": "Received 3 items: First, Second, Third")
    assert output =~ ~s("sequence": 3)
    assert output =~ ~s("isFinal": true)
  end

  test "Bidirectional Streaming: Client and server stream messages asynchronously", %{
    grpcurl: grpcurl
  } do
    frames = ~s({"message": "Alpha", "sequence": 1}\n{"message": "Beta", "sequence": 2}\n{"message": "Gamma", "sequence": 3})

    {output, 0} =
      System.cmd(
        grpcurl,
        [
          "-plaintext",
          "-proto",
          @proto_file,
          "-d",
          frames,
          "127.0.0.1:#{@port}",
          "streaming.StreamingService/BidirectionalStreamingCall"
        ],
        stderr_to_stdout: true
      )

    assert output =~ ~s("result": "BidiEcho: Alpha")
    assert output =~ ~s("sequence": 10)
    assert output =~ ~s("result": "BidiEcho: Beta")
    assert output =~ ~s("sequence": 20)
    assert output =~ ~s("result": "BidiEcho: Gamma")
    assert output =~ ~s("sequence": 30)
  end
end
