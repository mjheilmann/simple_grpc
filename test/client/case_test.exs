defmodule SimpleGrpc.CaseTest do
  use SimpleGrpc.Case, plug: SimpleGrpc.Plug

  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Schema
  alias SimpleGrpc.Registry

  @proto_file "test/fixtures/protos/streaming/streaming_service.proto"

  defmodule CaseSampleServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/streaming/streaming_service.proto"

    def unary_call(%{message: msg, sequence: seq}, _stream) do
      {:ok, %{result: "CaseEcho: " <> msg, sequence: seq + 10, is_final: true}}
    end

    def server_streaming_call(%{message: msg, sequence: seq}, _stream) do
      1..2
      |> Stream.map(fn i ->
        %{result: "CaseStream #{i}: #{msg}", sequence: seq + i, is_final: i == 2}
      end)
    end

    def client_streaming_call(req, _stream), do: req
    def bidirectional_streaming_call(req, _stream), do: req
  end

  setup_all do
    {:ok, schema} = Schema.from_file(@proto_file)
    %{schema: schema}
  end

  setup do
    Registry.clear()
    SimpleGrpc.register_service(CaseSampleServer)
    :ok
  end

  test "uses auto-injected in-memory conn from SimpleGrpc.Case", %{conn: conn, schema: schema} do
    req_bytes = Protobuf.encode!("StreamRequest", %{message: "TestRunner", sequence: 5}, schema)
    path = "/streaming.StreamingService/UnaryCall"

    {:ok, resp_bytes, headers, _trailers} = grpc_call(conn, path, req_bytes)

    assert is_list(headers)
    {:ok, decoded} = Protobuf.decode("StreamResponse", resp_bytes, schema)
    assert decoded.result == "CaseEcho: TestRunner"
    assert decoded.sequence == 15
    assert decoded.is_final == true
  end

  test "streams via grpc_stream helper", %{conn: conn, schema: schema} do
    req_bytes = Protobuf.encode!("StreamRequest", %{message: "StreamTest", sequence: 100}, schema)
    path = "/streaming.StreamingService/ServerStreamingCall"

    {:ok, resp_stream, _headers, _trailers} = grpc_stream(conn, path, req_bytes)

    items =
      resp_stream
      |> Enum.map(fn chunk ->
        {:ok, decoded} = Protobuf.decode("StreamResponse", chunk, schema)
        decoded
      end)

    assert length(items) == 2
    assert Enum.at(items, 0).result == "CaseStream 1: StreamTest"
    assert Enum.at(items, 1).result == "CaseStream 2: StreamTest"
  end
end
