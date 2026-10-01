defmodule SimpleGrpc.GrpcurlTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.{Registry, Schema}

  @port 50099
  @proto_file "priv/protos/streaming/streaming_service.proto"

  defmodule StreamingServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    def unary_call(%{message: msg, sequence: seq}, _stream) do
      {:ok, %{result: "Processed: " <> msg, sequence: seq + 1, is_final: true}}
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
        "-proto",
        @proto_file,
        "-d",
        ~s({"message": "Hello from grpcurl!", "sequence": 10}),
        "127.0.0.1:#{@port}",
        "streaming.StreamingService/UnaryCall"
      ])

    assert output =~ ~s("result": "Processed: Hello from grpcurl!")
    assert output =~ ~s("sequence": 11)
    assert output =~ ~s("isFinal": true)
  end

  test "grpcurl calls dynamically registered service and supports runtime hot-replacement", %{
    grpcurl: grpcurl
  } do
    # 1. Register a new service dynamically at runtime
    {:ok, schema} = Schema.from_file(@proto_file)

    SimpleGrpc.register_service(schema, %{
      unary_call: fn %{message: m, sequence: s} ->
        {:ok, %{result: "dynamic_v1: " <> m, sequence: s * 2, is_final: true}}
      end
    })

    {output1, 0} =
      System.cmd(grpcurl, [
        "-plaintext",
        "-proto",
        @proto_file,
        "-d",
        ~s({"message": "Runtime Test", "sequence": 5}),
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
        "-proto",
        @proto_file,
        "-d",
        ~s({"message": "Runtime Test", "sequence": 5}),
        "127.0.0.1:#{@port}",
        "streaming.StreamingService/UnaryCall"
      ])

    assert output2 =~ ~s("result": "dynamic_v2: Runtime Test")
    assert output2 =~ ~s("sequence": 50)
  end

  test "grpcurl receives standard gRPC error status and message from handler", %{grpcurl: grpcurl} do
    # Register a dynamic handler that returns a NotFound (code 5) gRPC error
    {:ok, schema} = Schema.from_file(@proto_file)

    SimpleGrpc.register_service(schema, %{
      unary_call: fn %{message: _msg} ->
        {:error, 5, "Requested user entity was not found in database"}
      end
    })

    {output, exit_code} =
      System.cmd(
        grpcurl,
        [
          "-plaintext",
          "-proto",
          @proto_file,
          "-d",
          ~s({"message": "find user 999", "sequence": 1}),
          "127.0.0.1:#{@port}",
          "streaming.StreamingService/UnaryCall"
        ],
        stderr_to_stdout: true
      )

    assert exit_code != 0
    assert output =~ "NotFound" or output =~ "Code: 5"
    assert output =~ "Requested user entity was not found in database"
  end

  test "grpcurl receives Unimplemented (12) for unknown service or method", %{grpcurl: grpcurl} do
    unregistered_proto = "priv/protos/global_service.proto"

    {output, exit_code} =
      System.cmd(
        grpcurl,
        [
          "-plaintext",
          "-proto",
          unregistered_proto,
          "-d",
          ~s({"data": "test"}),
          "127.0.0.1:#{@port}",
          "GlobalService/GlobalMethod"
        ],
        stderr_to_stdout: true
      )

    assert exit_code != 0
    assert output =~ "Unimplemented" or output =~ "Code: 12"
    assert output =~ "unknown service or method"
  end

  test "grpcurl sends and handler receives HTTP/2 request headers / metadata", %{grpcurl: grpcurl} do
    # Register a handler that extracts custom header from conn
    {:ok, schema} = Schema.from_file(@proto_file)

    SimpleGrpc.register_service(schema, %{
      unary_call: fn %{message: msg}, conn ->
        headers = if conn, do: conn.req_headers, else: []
        api_key = Enum.find_value(headers, "none", fn {k, v} -> if k == "x-api-key", do: v end)

        {:ok, %{result: "Auth: #{api_key} | Msg: #{msg}", sequence: 100, is_final: true}}
      end
    })

    {output, 0} =
      System.cmd(grpcurl, [
        "-plaintext",
        "-proto",
        @proto_file,
        "-H",
        "x-api-key: my-super-secret-token-123",
        "-d",
        ~s({"message": "Authorized request", "sequence": 1}),
        "127.0.0.1:#{@port}",
        "streaming.StreamingService/UnaryCall"
      ])

    assert output =~ ~s("result": "Auth: my-super-secret-token-123 | Msg: Authorized request")
    assert output =~ ~s("sequence": 100)
  end

  test "grpcurl invokes service with multi-file imports, nested submessages, enums, and WKT Timestamp",
       %{grpcurl: grpcurl} do
    proto_imports = "priv/protos/imports_test/imports_test.proto"
    {:ok, schema} = Schema.from_file(proto_imports)

    SimpleGrpc.register_service(schema, %{
      create_user: fn %{name: name, email: _email, address: addr, location: loc, registered_at: reg_at} ->
        {:ok,
         %{
           user_id: "user_created_#{name}",
           priority: :CRITICAL,
           created_at: %{
             seconds: (reg_at && reg_at.seconds) || 1_700_000_000,
             nanos: 0
           },
           profile: %{
             billing_address: addr,
             shipping_address: addr,
             recent_locations: [loc]
           }
         }}
      end
    })

    request_payload = ~s({
      "name": "Alice Wonderland",
      "email": "alice@example.com",
      "address": {
        "street": "100 Market St",
        "city": "San Francisco",
        "state": "CA",
        "postalCode": "94105",
        "country": "USA"
      },
      "location": {
        "latitude": 37.7937,
        "longitude": -122.3965,
        "altitude": 10.5
      },
      "registeredAt": "2026-10-01T12:00:00Z"
    })

    {output, 0} =
      System.cmd(
        grpcurl,
        [
          "-plaintext",
          "-proto",
          proto_imports,
          "-import-path",
          "priv/protos",
          "-d",
          request_payload,
          "127.0.0.1:#{@port}",
          "imports_test.ImportTestService/CreateUser"
        ],
        stderr_to_stdout: true
      )

    assert output =~ ~s("userId": "user_created_Alice Wonderland")
    assert output =~ ~s("CRITICAL")
    assert output =~ ~s("100 Market St")
    assert output =~ ~s("San Francisco")
  end

  describe "gRPC Server Reflection over HTTP/2 (Zero local -proto files)" do
    setup do
      # Enable reflection on the live server
      SimpleGrpc.enable_reflection()
      :ok
    end

    test "grpcurl list discovers all services via server reflection without -proto flag", %{
      grpcurl: grpcurl
    } do
      {output, 0} =
        System.cmd(grpcurl, ["-plaintext", "127.0.0.1:#{@port}", "list"],
          stderr_to_stdout: true
        )

      assert output =~ "streaming.StreamingService"
      assert output =~ "grpc.reflection.v1.ServerReflection"
    end

    test "grpcurl describe inspects service schema via server reflection without -proto flag", %{
      grpcurl: grpcurl
    } do
      {output, 0} =
        System.cmd(
          grpcurl,
          ["-plaintext", "127.0.0.1:#{@port}", "describe", "streaming.StreamingService"],
          stderr_to_stdout: true
        )

      assert output =~ "service StreamingService"
      assert output =~ "rpc UnaryCall"
    end

    test "grpcurl invokes RPC directly via server reflection without passing -proto file", %{
      grpcurl: grpcurl
    } do
      {output, 0} =
        System.cmd(
          grpcurl,
          [
            "-plaintext",
            "-d",
            ~s({"message": "Discovered via Reflection!", "sequence": 100}),
            "127.0.0.1:#{@port}",
            "streaming.StreamingService/UnaryCall"
          ],
          stderr_to_stdout: true
        )

      assert output =~ ~s("result": "Processed: Discovered via Reflection!")
      assert output =~ ~s("sequence": 101)
      assert output =~ ~s("isFinal": true)
    end
  end
end
