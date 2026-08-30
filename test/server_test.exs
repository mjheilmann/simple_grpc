defmodule SimpleGrpc.ServerTest do
  use ExUnit.Case

  alias SimpleGrpc.ServerTest.{
    SampleEchoServer,
    SampleStreamingServer,
    SampleNestedServer,
    SampleDeprecatedServer,
    SampleEmptyServer,
    SampleCallbackServer
  }

  defmodule SampleEmptyServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/empty_service/empty_service.proto"
  end

  defmodule SampleEchoServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/custom_prefix/custom_prefix_service.proto",
      handlers: [
        echo: fn %{message: msg}, _stream ->
          %{reply: "Echo: " <> msg}
        end
      ]
  end

  defmodule SampleCallbackServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/custom_prefix/custom_prefix_service.proto",
      handlers: [
        echo: :handle_echo
      ]

    def handle_echo(%{message: msg}) do
      %{reply: "Callback: " <> msg}
    end
  end

  defmodule SampleStreamingServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto",
      handlers: [
        unary_call: fn req, _stream ->
          %{result: "Processed: " <> req.message, sequence: req.sequence, is_final: true}
        end,
        server_streaming_call: fn req, _stream ->
          [result: "Stream: " <> req.message, sequence: req.sequence, is_final: true]
        end,
        client_streaming_call: fn req, _stream ->
          %{result: "ClientStream: " <> req.message, sequence: req.sequence, is_final: true}
        end,
        bidirectional_streaming_call: fn req, _stream ->
          %{result: "Bidi: " <> req.message, sequence: req.sequence, is_final: true}
        end
      ]
  end

  defmodule SampleNestedServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/nested/nested_messages.proto",
      handlers: [
        process_nested: fn req, _ -> req end
      ]
  end

  defmodule SampleDeprecatedServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/deprecated_fields/deprecated_fields.proto",
      handlers: [
        process: fn req, _ -> req end
      ]
  end

  test "Server Compile fails when no service is present" do
    server = """
    defmodule TestNoService do
      use SimpleGrpc.Server, 
        server_file: "priv/protos/common/common_types.proto"
    end
    """

    assert_raise ArgumentError, ~r/service definition not found/, fn ->
      Code.eval_string(server)
    end
  end

  test "Server Compile succeeds when a service is present with no RPCs" do
    assert SampleEmptyServer.__service_name__() == "EmptyService"
    assert SampleEmptyServer.__rpcs__() == []
    assert SampleEmptyServer.__handlers__() == []
    assert SampleEmptyServer.__messages__() == %{}
  end

  test "Server Compile fails when handlers are missing entirely" do
    server = """
    defmodule TestMissingAllHandlers do
      use SimpleGrpc.Server,
        server_file: "priv/protos/custom_prefix/custom_prefix_service.proto"
    end
    """

    assert_raise ArgumentError, ~r/Missing handler for RPC call\(s\): "Echo"/, fn ->
      Code.eval_string(server)
    end
  end

  test "Server Compile fails when some handlers are missing" do
    server = """
    defmodule TestMissingSomeHandlers do
      use SimpleGrpc.Server,
        server_file: "priv/protos/streaming/streaming_service.proto",
        handlers: [
          unary_call: fn req, _stream -> req end
        ]
    end
    """

    assert_raise ArgumentError, ~r/Missing handler for RPC call\(s\): "ServerStreamingCall", "ClientStreamingCall", "BidirectionalStreamingCall"/, fn ->
      Code.eval_string(server)
    end
  end

  test "Server Compile succeeds when handlers are provided with snake_case atoms" do
    assert SampleEchoServer.__service_name__() == "PrefixService"
    assert length(SampleEchoServer.__rpcs__()) == 1

    [rpc] = SampleEchoServer.__rpcs__()
    assert rpc.name == "Echo"
    assert rpc.input == "EchoRequest"
    assert rpc.output == "EchoResponse"
    assert rpc.request_stream == false
    assert rpc.response_stream == false
  end

  test "Server Compile succeeds when handlers are provided with PascalCase atoms or map" do
    server = """
    defmodule TestPascalCaseHandlers do
      use SimpleGrpc.Server,
        server_file: "priv/protos/custom_prefix/custom_prefix_service.proto",
        handlers: [
          Echo: fn req -> req end
        ]
    end

    defmodule TestMapHandlers do
      use SimpleGrpc.Server,
        server_file: "priv/protos/custom_prefix/custom_prefix_service.proto",
        handlers: %{
          "Echo" => fn req -> req end
        }
    end
    """

    Code.eval_string(server)
    assert apply(TestPascalCaseHandlers, :__service_name__, []) == "PrefixService"
    assert apply(TestMapHandlers, :__service_name__, []) == "PrefixService"
  end

  test "Generates message modules, structs, and constructors for input/output messages" do
    req = %SampleEchoServer.EchoRequest{message: "ping"}
    assert req.message == "ping"

    req_from_new = SampleEchoServer.EchoRequest.new(%{message: "ping_new"})
    assert req_from_new.message == "ping_new"

    resp = %SampleEchoServer.EchoResponse{reply: "pong"}
    assert resp.reply == "pong"

    assert SampleEchoServer.__messages__() == %{
      "EchoRequest" => SampleEchoServer.EchoRequest,
      "EchoResponse" => SampleEchoServer.EchoResponse
    }
  end

  test "Extracts message field metadata via __fields__/0 and __field__/1" do
    assert SampleEchoServer.EchoRequest.__message_name__() == "EchoRequest"
    [field] = SampleEchoServer.EchoRequest.__fields__()

    assert field.name == :message
    assert field.type == "string"
    assert field.number == 1
    assert field.kind == :field
    assert field.deprecated == false

    assert SampleEchoServer.EchoRequest.__field__(:message) == field
    assert SampleEchoServer.EchoRequest.__field__("message") == field
    assert SampleEchoServer.EchoRequest.__field__(:unknown) == nil
  end

  test "Handles streaming RPC modifiers and multiple RPCs in service" do
    rpcs = SampleStreamingServer.__rpcs__()
    assert length(rpcs) == 4

    unary = Enum.find(rpcs, &(&1.name == "UnaryCall"))
    assert unary.request_stream == false
    assert unary.response_stream == false

    server_stream = Enum.find(rpcs, &(&1.name == "ServerStreamingCall"))
    assert server_stream.request_stream == false
    assert server_stream.response_stream == true

    client_stream = Enum.find(rpcs, &(&1.name == "ClientStreamingCall"))
    assert client_stream.request_stream == true
    assert client_stream.response_stream == false

    bidi_stream = Enum.find(rpcs, &(&1.name == "BidirectionalStreamingCall"))
    assert bidi_stream.request_stream == true
    assert bidi_stream.response_stream == true

    req = %SampleStreamingServer.StreamRequest{message: "hello", sequence: 1}
    assert req.message == "hello"
    assert req.sequence == 1

    resp = %SampleStreamingServer.StreamResponse{result: "world", sequence: 1, is_final: true}
    assert resp.result == "world"
    assert resp.sequence == 1
    assert resp.is_final == true
  end

  test "Handles nested message modules, maps, oneofs, and repeated fields" do
    mid = %SampleNestedServer.OuterMessage.MiddleMessage{middle_field: "mid_val"}
    assert mid.middle_field == "mid_val"

    deep = %SampleNestedServer.OuterMessage.MiddleMessage.InnerMessage.DeepMessage{deep_field: "deep_val"}
    assert deep.deep_field == "deep_val"

    fields = SampleNestedServer.OuterMessage.__fields__()

    repeated_field = Enum.find(fields, &(&1.name == :middle_list))
    assert repeated_field.prefix == :repeated
    assert repeated_field.type == "MiddleMessage"
    assert repeated_field.number == 3

    map_field = Enum.find(fields, &(&1.name == :nested_map))
    assert map_field.kind == :map
    assert map_field.key_type == "string"
    assert map_field.val_type == "MiddleMessage"
    assert map_field.number == 4

    oneof_field = Enum.find(fields, &(&1.name == :nested_oneof))
    assert oneof_field.kind == :oneof
    assert length(oneof_field.fields) == 3
  end

  test "Handles deprecated fields and field options" do
    active_field = SampleDeprecatedServer.DeprecatedRequest.__field__(:active_field)
    assert active_field.deprecated == false

    legacy_field = SampleDeprecatedServer.DeprecatedRequest.__field__(:legacy_field)
    assert legacy_field.deprecated == true
  end

  test "Dispatches RPC calls with plain maps, pattern matching, and response coercion" do
    response = SampleEchoServer.dispatch(:echo, %{message: "Hello World"})
    assert response == %SampleEchoServer.EchoResponse{reply: "Echo: Hello World"}

    # Keyword list input
    response_kw = SampleEchoServer.dispatch("Echo", [message: "Keyword World"])
    assert response_kw == %SampleEchoServer.EchoResponse{reply: "Echo: Keyword World"}

    # Callback function dispatch
    callback_response = SampleCallbackServer.dispatch(:echo, %{message: "Callback Test"})
    assert callback_response == %SampleCallbackServer.EchoResponse{reply: "Callback: Callback Test"}
  end

  test "Dispatches streaming service RPCs with map and keyword return values" do
    unary_resp = SampleStreamingServer.dispatch(:unary_call, %{message: "test", sequence: 5})
    assert unary_resp == %SampleStreamingServer.StreamResponse{
             result: "Processed: test",
             sequence: 5,
             is_final: true
           }

    server_stream_resp =
      SampleStreamingServer.dispatch(:server_streaming_call, %{message: "stream", sequence: 10})

    assert server_stream_resp == %SampleStreamingServer.StreamResponse{
             result: "Stream: stream",
             sequence: 10,
             is_final: true
           }
  end

  test "build_request and build_response helpers populate defaults and validate fields" do
    req = SampleEchoServer.build_request(:echo, %{message: "Ping"})
    assert req == %SampleEchoServer.EchoRequest{message: "Ping"}

    # Defaults populated
    empty_resp = SampleEchoServer.build_response(:echo, %{})
    assert empty_resp == %SampleEchoServer.EchoResponse{reply: ""}

    # Unknown field raises ArgumentError
    assert_raise ArgumentError, ~r/Unknown field\(s\) \[:non_existent\]/, fn ->
      SampleEchoServer.build_request(:echo, %{non_existent: "error"})
    end
  end
end
