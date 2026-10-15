defmodule SimpleGrpc.ServerTest do
  use ExUnit.Case

  alias SimpleGrpc.ServerTest.{
    SampleEchoServer,
    SampleStreamingServer,
    SampleNestedServer,
    SampleDeprecatedServer,
    SampleEmptyServer,
    SampleGlobalServer,
    SampleDelegatedServer
  }

  defmodule SampleEmptyServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/empty_service/empty_service.proto"
  end

  defmodule SampleEchoServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/custom_prefix/custom_prefix_service.proto"

    # In-module handler callback
    def echo(%{message: msg}, _stream) do
      %{reply: "Echo: " <> msg}
    end
  end

  defmodule SampleGlobalServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/global_service.proto"

    # In-module handler with 1-arity
    def global_method(%{data: data}) do
      %{result: "Global: " <> data}
    end
  end

  defmodule SampleDelegatedServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/custom_prefix/custom_prefix_service.proto",
      handlers: [
        echo: fn %{message: msg} -> %{reply: "Delegated: " <> msg} end
      ]
  end

  defmodule SampleStreamingServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/streaming/streaming_service.proto"

    def unary_call(req, _stream) do
      %{result: "Processed: " <> req.message, sequence: req.sequence, is_final: true}
    end

    def server_streaming_call(req, _stream) do
      [result: "Stream: " <> req.message, sequence: req.sequence, is_final: true]
    end

    def client_streaming_call(req, _stream) do
      %{result: "ClientStream: " <> req.message, sequence: req.sequence, is_final: true}
    end

    def bidirectional_streaming_call(req, _stream) do
      %{result: "Bidi: " <> req.message, sequence: req.sequence, is_final: true}
    end
  end

  defmodule SampleNestedServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/nested/nested_messages.proto"

    def process_nested(req, _stream) do
      req
    end
  end

  defmodule SampleDeprecatedServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/deprecated_fields/deprecated_fields.proto"

    def process(req, _stream) do
      req
    end
  end

  test "Server Compile fails when no service is present" do
    server = """
    defmodule TestNoService do
      use SimpleGrpc.Server, 
        server_file: "test/fixtures/protos/common/common_types.proto"
    end
    """

    assert_raise CompileError, ~r/no service definition found/, fn ->
      Code.eval_string(server)
    end
  end

  test "Server Compile succeeds when a service is present with no RPCs" do
    assert SampleEmptyServer.__service_name__() == "EmptyService"
    assert SampleEmptyServer.__rpcs__() == []
    assert SampleEmptyServer.__handlers__() == []
    assert SampleEmptyServer.__messages__() == %{}
  end

  test "Server Compile fails when in-module handler function is missing" do
    server = """
    defmodule TestMissingInModuleHandler do
      use SimpleGrpc.Server,
        server_file: "test/fixtures/protos/custom_prefix/custom_prefix_service.proto"
    end
    """

    assert_raise CompileError, ~r/Missing handler for RPC call\(s\): "Echo"/, fn ->
      Code.eval_string(server)
    end
  end

  test "Server Compile fails when some in-module handlers are missing" do
    server = """
    defmodule TestMissingSomeInModuleHandlers do
      use SimpleGrpc.Server,
        server_file: "test/fixtures/protos/streaming/streaming_service.proto"

      def unary_call(req, _stream), do: req
    end
    """

    assert_raise CompileError,
                 ~r/Missing handler for RPC call\(s\): "ServerStreamingCall", "ClientStreamingCall", "BidirectionalStreamingCall"/,
                 fn ->
                   Code.eval_string(server)
                 end
  end

  test "Server Compile succeeds with in-module handler callbacks" do
    assert SampleEchoServer.__service_name__() == "PrefixService"
    assert SampleEchoServer.__grpc_package__() == "custom_prefix"
    assert SampleEchoServer.__grpc_service_path__() == "/custom_prefix.PrefixService"
    assert length(SampleEchoServer.__rpcs__()) == 1

    [rpc] = SampleEchoServer.__rpcs__()
    assert rpc.name == "Echo"
    assert rpc.input == "EchoRequest"
    assert rpc.output == "EchoResponse"
    assert rpc.request_stream == false
    assert rpc.response_stream == false
  end

  test "Server Compile succeeds with delegated opts handlers" do
    assert SampleDelegatedServer.__service_name__() == "PrefixService"
    response = SampleDelegatedServer.dispatch(:echo, %{message: "Test"})
    assert response == %SampleDelegatedServer.EchoResponse{reply: "Delegated: Test"}
  end

  test "Generates canonical gRPC paths and route matching metadata" do
    # Package-scoped service path
    assert SampleEchoServer.grpc_path(:echo) == "/custom_prefix.PrefixService/Echo"
    assert SampleEchoServer.grpc_path("Echo") == "/custom_prefix.PrefixService/Echo"
    assert SampleEchoServer.matches_path?("/custom_prefix.PrefixService/Echo") == true
    assert SampleEchoServer.matches_path?("/custom_prefix.PrefixService/Unknown") == false
    assert SampleEchoServer.matches_path?("/other.Service/Method") == false

    info = SampleEchoServer.route_info("/custom_prefix.PrefixService/Echo")
    assert info.rpc_name == "Echo"
    assert info.service == "PrefixService"
    assert info.package == "custom_prefix"
    assert info.input_module == SampleEchoServer.EchoRequest
    assert info.output_module == SampleEchoServer.EchoResponse

    assert SampleEchoServer.route_info("/non/existent/path") == nil

    # Global service (no package)
    assert SampleGlobalServer.grpc_path(:global_method) == "/GlobalService/GlobalMethod"
    assert SampleGlobalServer.matches_path?("/GlobalService/GlobalMethod") == true
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

    deep = %SampleNestedServer.OuterMessage.MiddleMessage.InnerMessage.DeepMessage{
      deep_field: "deep_val"
    }

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

  test "Dispatches RPC calls by RPC name and by canonical gRPC HTTP path" do
    # Dispatch by atom RPC name
    response = SampleEchoServer.dispatch(:echo, %{message: "Hello World"})
    assert response == %SampleEchoServer.EchoResponse{reply: "Echo: Hello World"}

    # Dispatch by string RPC name
    response_str = SampleEchoServer.dispatch("Echo", %{message: "String RPC"})
    assert response_str == %SampleEchoServer.EchoResponse{reply: "Echo: String RPC"}

    # Dispatch by full gRPC HTTP path
    response_path =
      SampleEchoServer.dispatch("/custom_prefix.PrefixService/Echo", %{message: "HTTP Path Call"})

    assert response_path == %SampleEchoServer.EchoResponse{reply: "Echo: HTTP Path Call"}

    # Keyword list input
    response_kw = SampleEchoServer.dispatch("Echo", message: "Keyword World")
    assert response_kw == %SampleEchoServer.EchoResponse{reply: "Echo: Keyword World"}

    # Global service dispatch
    global_response = SampleGlobalServer.dispatch(:global_method, %{data: "Global Payload"})
    assert global_response == %SampleGlobalServer.GlobalResponse{result: "Global: Global Payload"}
  end

  test "Dispatches streaming service RPCs with in-module handlers" do
    unary_resp = SampleStreamingServer.dispatch(:unary_call, %{message: "test", sequence: 5})

    assert unary_resp == %SampleStreamingServer.StreamResponse{
             result: "Processed: test",
             sequence: 5,
             is_final: true
           }

    server_stream_resp =
      SampleStreamingServer.dispatch("/streaming.StreamingService/ServerStreamingCall", %{
        message: "stream",
        sequence: 10
      })

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

  test "Coercer and Message.new handle optional fields with explicit presence" do
    {:ok, schema} =
      SimpleGrpc.Schema.from_file("test/fixtures/protos/scalar_types/scalar_types.proto")

    msg = SimpleGrpc.Schema.message(schema, "ScalarRequest")
    ast = SimpleGrpc.Server.Message.generate_module_ast(msg, OptionalTestNamespace)
    Code.eval_quoted(ast)

    mod = Module.concat(OptionalTestNamespace, "ScalarRequest")

    # Empty attributes -> optional fields default to nil, non-optional fields default to scalar defaults
    struct_empty = mod.new(%{})
    assert is_nil(struct_empty.optional_string)
    assert is_nil(struct_empty.optional_int)
    assert struct_empty.string_field == ""
    assert struct_empty.int32_field == 0
    assert struct_empty.bool_field == false

    # Explicit nil attributes -> optional fields stay nil
    struct_nil = mod.new(%{optional_string: nil, optional_int: nil})

    assert is_nil(struct_nil.optional_string)
    assert is_nil(struct_nil.optional_int)

    # Explicit default attributes -> optional fields preserve explicit defaults
    struct_defaults = mod.new(%{optional_string: "", optional_int: 0})

    assert struct_defaults.optional_string == ""
    assert struct_defaults.optional_int == 0

    # Explicit non-default attributes
    struct_custom = mod.new(%{optional_string: "hello", optional_int: 42})

    assert struct_custom.optional_string == "hello"
    assert struct_custom.optional_int == 42
  end

  test "Coercer resolves enum atoms, strings, and integers and populates zero-atom defaults" do
    {:ok, schema} =
      SimpleGrpc.Schema.from_file("test/fixtures/protos/edge_cases/edge_cases.proto")

    msg = SimpleGrpc.Schema.message(schema, "ComplicatedRequest")
    schema_escaped = Macro.escape(schema)

    # Define a parent namespace that exposes the schema so __schema__/0 forwards work
    parent_ast =
      quote do
        defmodule EnumTestNamespace do
          def __schema__, do: unquote(schema_escaped)
        end
      end

    Code.eval_quoted(parent_ast)
    ast = SimpleGrpc.Server.Message.generate_module_ast(msg, EnumTestNamespace)
    Code.eval_quoted(ast)

    mod = Module.concat(EnumTestNamespace, "ComplicatedRequest")

    # When created empty, non-optional enum field defaults to zero atom
    empty_struct = mod.new(%{})
    assert empty_struct.status == :DETAILED_STATUS_UNSPECIFIED

    # Atom value
    atom_struct = mod.new(%{status: :status_active})
    assert atom_struct.status == :status_active

    # String value
    str_struct = mod.new(%{status: "STATUS_INACTIVE"})
    assert str_struct.status == :STATUS_INACTIVE

    # Integer value
    int_struct = mod.new(%{status: 3})
    assert int_struct.status == :Status_Pending
  end

  test "Dispatcher normalizes atom error tuples ({:error, :not_found, msg}) into standard integer codes" do
    assert {:error, 5, "User not found"} =
             SimpleGrpc.Dispatcher.invoke_handler(
               fn _req -> {:error, :not_found, "User not found"} end,
               "GetUser",
               %{},
               nil
             )

    assert {:error, 3, "Invalid email"} =
             SimpleGrpc.Dispatcher.invoke_handler(
               fn _req -> {:error, :invalid_argument, "Invalid email"} end,
               "CreateUser",
               %{},
               nil
             )

    assert {:error, 16, "unauthenticated"} =
             SimpleGrpc.Dispatcher.invoke_handler(
               fn _req -> {:error, :unauthenticated} end,
               "SecureCall",
               %{},
               nil
             )
  end

  test "Server generates struct modules for all messages defined in proto, including non-RPC messages" do
    # ComplexNested is not used in NestedService RPCs, but is generated as a struct
    assert Code.ensure_loaded?(SampleNestedServer.ComplexNested)
    assert Code.ensure_loaded?(SampleNestedServer.OuterMessage)

    struct = SampleNestedServer.ComplexNested.new(%{status: :ACTIVE})
    assert struct.status == :ACTIVE
    assert is_struct(struct, SampleNestedServer.ComplexNested)
  end

  test "SimpleGrpc.Plug correctly initializes routes when configured with server: ServerModule" do
    plug_state = SimpleGrpc.Plug.init(server: SampleEchoServer)
    assert map_size(plug_state.routes) > 0
    assert Map.has_key?(plug_state.routes, "/custom_prefix.PrefixService/Echo")

    # Send in-memory request through Plug
    {:ok, conn} =
      SimpleGrpc.Client.connect({SimpleGrpc.Plug, server: SampleEchoServer}, adapter: :plug)

    {:ok, schema} =
      SimpleGrpc.Schema.from_file(
        "test/fixtures/protos/custom_prefix/custom_prefix_service.proto"
      )

    req_bytes = SimpleGrpc.Protobuf.encode!("EchoRequest", %{message: "ServerOptTest"}, schema)

    {:ok, resp_bytes, _headers, _trailers} =
      SimpleGrpc.Client.send_unary(conn, "/custom_prefix.PrefixService/Echo", req_bytes)

    {:ok, decoded} = SimpleGrpc.Protobuf.decode("EchoResponse", resp_bytes, schema)
    assert decoded.reply == "Echo: ServerOptTest"
  end
end
