defmodule SimpleGrpc.ErrorDetailsTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.{Status, ErrorDetails, Framing}
  alias SimpleGrpc.RPC.Status, as: RPCStatus

  # ===========================================================================
  # Test Server & Client Setup
  # ===========================================================================

  defmodule ErrorTestServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    alias SimpleGrpc.ErrorDetails
    alias SimpleGrpc.RPC.Status, as: RPCStatus

    def unary_call(%{message: "throw_bad_request"}, _stream) do
      details = [
        ErrorDetails.bad_request([
          %{field: "username", description: "Username cannot be empty"},
          %{field: "email", description: "Invalid email format"}
        ]),
        ErrorDetails.localized_message("en-US", "Please correct the form errors and retry.")
      ]

      {:error, :invalid_argument, "Validation failed", details}
    end

    def unary_call(%{message: "throw_quota_exceeded"}, _stream) do
      status =
        RPCStatus.new(:resource_exhausted, "Rate limit exceeded", [
          ErrorDetails.quota_failure([
            %{subject: "client:127.0.0.1", description: "100 requests per minute limit reached"}
          ]),
          ErrorDetails.retry_info(30),
          ErrorDetails.help([
            %{description: "Rate limit docs", url: "https://example.com/docs/rate-limits"}
          ])
        ])

      {:error, status}
    end

    def unary_call(%{message: "throw_error_info"}, _stream) do
      details = [
        ErrorDetails.error_info("API_KEY_EXPIRED", "auth.googleapis.com", %{
          "account_id" => "acc_12345",
          "service" => "streaming.StreamingService"
        }),
        ErrorDetails.debug_info(
          ["Handler.ex:42", "Dispatcher.ex:108"],
          "API key revoked at 2026-10-10"
        )
      ]

      {:error, :unauthenticated, "Authentication failed", details}
    end

    def unary_call(%{message: "throw_plain_error"}, _stream) do
      {:error, :not_found, "Resource not found"}
    end

    def unary_call(req, _stream) do
      {:ok, %{result: "Success: " <> req.message, sequence: req.sequence, is_final: true}}
    end

    def server_streaming_call(%{message: "stream_error_with_details"}, _stream) do
      throw(
        {:error, :permission_denied, "Access denied to stream",
         [
           ErrorDetails.error_info("PERMISSION_DENIED", "iam.googleapis.com", %{
             "role" => "viewer"
           }),
           ErrorDetails.precondition_failure([
             %{type: "IAM_ROLE", subject: "user:alice", description: "Requires editor role"}
           ])
         ]}
      )
    end

    def server_streaming_call(req, _stream) do
      [%{result: "Stream: " <> req.message, sequence: 1, is_final: true}]
    end

    def client_streaming_call(_req_stream, _stream) do
      {:ok, %{result: "OK", sequence: 1, is_final: true}}
    end

    def bidirectional_streaming_call(_req_stream, _stream) do
      [%{result: "BidiOK", sequence: 1, is_final: true}]
    end
  end

  defmodule ErrorTestClient do
    use SimpleGrpc.Client,
      client_file: "priv/protos/streaming/streaming_service.proto",
      adapter: SimpleGrpc.Client.Adapters.Plug
  end

  # ===========================================================================
  # 1. SimpleGrpc.RPC.Status Struct & Unit Tests
  # ===========================================================================

  describe "SimpleGrpc.RPC.Status struct construction" do
    test "constructs status with code atom and message" do
      status = RPCStatus.new(:invalid_argument, "Invalid argument")
      assert status.code == 3
      assert status.status_code == :invalid_argument
      assert status.message == "Invalid argument"
      assert status.details == []
    end

    test "constructs status with integer code" do
      status = RPCStatus.new(5, "Not found")
      assert status.code == 5
      assert status.status_code == :not_found
      assert status.message == "Not found"
    end

    test "add_detail/2 appends detail item" do
      status =
        RPCStatus.new(:resource_exhausted, "Quota exceeded")
        |> RPCStatus.add_detail(ErrorDetails.retry_info(10))

      assert length(status.details) == 1
    end
  end

  # ===========================================================================
  # 2. SimpleGrpc.ErrorDetails Builders & Any Serialization
  # ===========================================================================

  describe "SimpleGrpc.ErrorDetails builders and packing" do
    test "error_info/3 builder" do
      info = ErrorDetails.error_info("USER_DISABLED", "accounts.example.com", %{user_id: 123})
      assert info.reason == "USER_DISABLED"
      assert info.domain == "accounts.example.com"
      assert info.metadata["user_id"] == "123"

      packed = ErrorDetails.pack(info)
      assert packed.type_url == "type.googleapis.com/google.rpc.ErrorInfo"
      assert is_binary(packed.value)

      unpacked = ErrorDetails.unpack(packed)
      assert unpacked.reason == "USER_DISABLED"
      assert unpacked.domain == "accounts.example.com"
      assert unpacked.__type__ == "google.rpc.ErrorInfo"
    end

    test "bad_request/1 builder with field violations" do
      bad_req =
        ErrorDetails.bad_request([
          {"username", "cannot be blank"},
          %{field: "password", description: "too short"}
        ])

      assert length(bad_req.field_violations) == 2
      assert Enum.at(bad_req.field_violations, 0).field == "username"
      assert Enum.at(bad_req.field_violations, 1).description == "too short"

      packed = ErrorDetails.pack(bad_req)
      assert packed.type_url == "type.googleapis.com/google.rpc.BadRequest"

      unpacked = ErrorDetails.unpack(packed)
      assert length(unpacked.field_violations) == 2
    end

    test "retry_info/1 builder" do
      retry = ErrorDetails.retry_info(5.5)
      assert retry.retry_delay.seconds == 5
      assert retry.retry_delay.nanos == 500_000_000

      packed = ErrorDetails.pack(retry)
      assert packed.type_url == "type.googleapis.com/google.rpc.RetryInfo"

      unpacked = ErrorDetails.unpack(packed)
      assert unpacked.retry_delay.seconds == 5
    end

    test "debug_info/2 builder" do
      debug = ErrorDetails.debug_info(["app.ex:10", "lib.ex:20"], "Stacktrace info")
      assert debug.stack_entries == ["app.ex:10", "lib.ex:20"]
      assert debug.detail == "Stacktrace info"

      packed = ErrorDetails.pack(debug)
      assert packed.type_url == "type.googleapis.com/google.rpc.DebugInfo"

      unpacked = ErrorDetails.unpack(packed)
      assert unpacked.stack_entries == ["app.ex:10", "lib.ex:20"]
    end

    test "quota_failure/1 builder" do
      quota = ErrorDetails.quota_failure([%{subject: "user:1", description: "Daily limit"}])
      assert length(quota.violations) == 1

      packed = ErrorDetails.pack(quota)
      assert packed.type_url == "type.googleapis.com/google.rpc.QuotaFailure"

      unpacked = ErrorDetails.unpack(packed)
      assert length(unpacked.violations) == 1
    end

    test "precondition_failure/1 builder" do
      precond =
        ErrorDetails.precondition_failure([
          %{type: "TOS", subject: "user:1", description: "Terms of service not accepted"}
        ])

      packed = ErrorDetails.pack(precond)
      assert packed.type_url == "type.googleapis.com/google.rpc.PreconditionFailure"

      unpacked = ErrorDetails.unpack(packed)
      assert Enum.at(unpacked.violations, 0).type == "TOS"
    end

    test "resource_info/4 builder" do
      res = ErrorDetails.resource_info("Bucket", "my-bucket", "user:admin", "Primary storage")
      assert res.resource_type == "Bucket"
      assert res.resource_name == "my-bucket"

      packed = ErrorDetails.pack(res)
      assert packed.type_url == "type.googleapis.com/google.rpc.ResourceInfo"

      unpacked = ErrorDetails.unpack(packed)
      assert unpacked.resource_name == "my-bucket"
    end

    test "help/1 builder" do
      h = ErrorDetails.help([{"Support link", "https://support.example.com"}])
      packed = ErrorDetails.pack(h)
      assert packed.type_url == "type.googleapis.com/google.rpc.Help"

      unpacked = ErrorDetails.unpack(packed)
      assert Enum.at(unpacked.links, 0).url == "https://support.example.com"
    end

    test "localized_message/2 builder" do
      loc = ErrorDetails.localized_message("es-ES", "Operación no permitida")
      packed = ErrorDetails.pack(loc)
      assert packed.type_url == "type.googleapis.com/google.rpc.LocalizedMessage"

      unpacked = ErrorDetails.unpack(packed)
      assert unpacked.message == "Operación no permitida"
    end

    test "request_info/2 builder" do
      req = ErrorDetails.request_info("req-12345", "server-us-east-1")
      packed = ErrorDetails.pack(req)
      assert packed.type_url == "type.googleapis.com/google.rpc.RequestInfo"

      unpacked = ErrorDetails.unpack(packed)
      assert unpacked.request_id == "req-12345"
    end
  end

  # ===========================================================================
  # 3. Status Base64 Encoding & Extraction
  # ===========================================================================

  describe "SimpleGrpc.Status binary metadata serialization" do
    test "to_status_details_bin and from_status_details_bin roundtrip" do
      details = [
        ErrorDetails.bad_request([%{field: "age", description: "must be >= 18"}]),
        ErrorDetails.localized_message("en-US", "Age verification required")
      ]

      status = RPCStatus.new(:invalid_argument, "Invalid Age", details)
      b64_str = Status.to_status_details_bin(status)
      assert is_binary(b64_str)

      assert {:ok, decoded_status} = Status.from_status_details_bin(b64_str)
      assert decoded_status.code == 3
      assert decoded_status.status_code == :invalid_argument
      assert decoded_status.message == "Invalid Age"
      assert length(decoded_status.details) == 2

      # Check unpacked detail types
      [d1, d2] = decoded_status.details
      assert d1.__type__ == "google.rpc.BadRequest"
      assert Enum.at(d1.field_violations, 0).field == "age"
      assert d2.__type__ == "google.rpc.LocalizedMessage"
      assert d2.message == "Age verification required"
    end

    test "extract_status/1 and extract_details/1 helpers" do
      b64 =
        Status.to_status_details_bin({
          :permission_denied,
          "Permission denied",
          [ErrorDetails.error_info("AUTHZ_FAIL", "auth.service.com")]
        })

      headers = [
        {"content-type", "application/grpc"},
        {"grpc-status", "7"},
        {"grpc-message", "Permission%20denied"},
        {"grpc-status-details-bin", b64}
      ]

      {code, msg, rich_status} = Status.extract_status(headers)
      assert code == 7
      assert msg == "Permission denied"
      assert %RPCStatus{} = rich_status
      assert rich_status.code == 7

      details = Status.extract_details(headers)
      assert length(details) == 1
      assert Enum.at(details, 0).reason == "AUTHZ_FAIL"
    end
  end

  # ===========================================================================
  # 4. Server-Side Plug Tests with Rich Error Details
  # ===========================================================================

  describe "Server-Side Plug grpc-status-details-bin response emission" do
    setup do
      plug_opts = SimpleGrpc.Plug.init(services: [ErrorTestServer])
      schema = ErrorTestServer.__schema__()
      %{plug_opts: plug_opts, schema: schema}
    end

    test "emits grpc-status-details-bin when handler returns 4-tuple error", %{
      plug_opts: plug_opts,
      schema: schema
    } do
      {:ok, req_bytes} =
        SimpleGrpc.Protobuf.encode(
          "streaming.StreamRequest",
          %{message: "throw_bad_request", sequence: 1},
          schema
        )

      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", Framing.frame(req_bytes))
        |> Plug.Conn.put_req_header("content-type", "application/grpc")
        |> SimpleGrpc.Plug.call(plug_opts)

      headers = conn.resp_headers
      {code, msg, rich_status} = Status.extract_status(headers)

      assert code == 3
      assert msg == "Validation failed"
      assert %RPCStatus{} = rich_status
      assert length(rich_status.details) == 2

      bad_req = Enum.find(rich_status.details, &(&1.__type__ == "google.rpc.BadRequest"))
      assert bad_req != nil
      assert length(bad_req.field_violations) == 2
    end

    test "emits grpc-status-details-bin when handler returns %SimpleGrpc.RPC.Status{}", %{
      plug_opts: plug_opts,
      schema: schema
    } do
      {:ok, req_bytes} =
        SimpleGrpc.Protobuf.encode(
          "streaming.StreamRequest",
          %{message: "throw_quota_exceeded", sequence: 1},
          schema
        )

      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", Framing.frame(req_bytes))
        |> Plug.Conn.put_req_header("content-type", "application/grpc")
        |> SimpleGrpc.Plug.call(plug_opts)

      headers = conn.resp_headers
      {code, msg, rich_status} = Status.extract_status(headers)

      assert code == 8
      assert msg == "Rate limit exceeded"
      assert %RPCStatus{} = rich_status
      assert length(rich_status.details) == 3

      quota = Enum.find(rich_status.details, &(&1.__type__ == "google.rpc.QuotaFailure"))
      assert quota != nil
      assert Enum.at(quota.violations, 0).subject == "client:127.0.0.1"
    end

    test "does not emit grpc-status-details-bin when plain error is returned without details", %{
      plug_opts: plug_opts,
      schema: schema
    } do
      {:ok, req_bytes} =
        SimpleGrpc.Protobuf.encode(
          "streaming.StreamRequest",
          %{message: "throw_plain_error", sequence: 1},
          schema
        )

      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", Framing.frame(req_bytes))
        |> Plug.Conn.put_req_header("content-type", "application/grpc")
        |> SimpleGrpc.Plug.call(plug_opts)

      headers = conn.resp_headers
      {code, msg, rich_status} = Status.extract_status(headers)

      assert code == 5
      assert msg == "Resource not found"
      assert rich_status == nil
    end
  end

  # ===========================================================================
  # 5. Client-Side High-Level and DSL Error Handling
  # ===========================================================================

  describe "Client High-Level Calls receive and decode rich status" do
    setup do
      {:ok, conn} =
        SimpleGrpc.Client.connect({SimpleGrpc.Plug, services: [ErrorTestServer]}, adapter: :plug)

      %{conn: conn}
    end

    test "unary call receiving rich status returns 4-tuple with %SimpleGrpc.RPC.Status{}", %{
      conn: conn
    } do
      assert {:error, 3, "Validation failed", %RPCStatus{} = rich_status} =
               ErrorTestClient.unary_call(conn, %{message: "throw_bad_request", sequence: 1})

      assert rich_status.code == 3
      assert rich_status.status_code == :invalid_argument
      assert length(rich_status.details) == 2

      details = Status.extract_details(rich_status)
      bad_req = Enum.find(details, &(&1.__type__ == "google.rpc.BadRequest"))
      assert bad_req != nil
      assert Enum.at(bad_req.field_violations, 0).field == "username"
    end

    test "unary call receiving plain error returns 3-tuple", %{conn: conn} do
      assert {:error, 5, "Resource not found"} =
               ErrorTestClient.unary_call(conn, %{message: "throw_plain_error", sequence: 1})
    end
  end

  # ===========================================================================
  # 6. End-to-End Network Socket Integration (Bandit + Mint)
  # ===========================================================================

  describe "End-to-End Rich Error Details over HTTP/2 Mint socket" do
    setup do
      server =
        start_supervised!(
          {Bandit,
           plug: {SimpleGrpc.Plug, services: [ErrorTestServer]},
           port: 0,
           scheme: :http,
           startup_log: false,
           thousand_island_options: [num_acceptors: 2]}
        )

      {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

      Process.sleep(50)

      schema = ErrorTestServer.__schema__()

      {:ok, client} =
        SimpleGrpc.Client.connect("http://127.0.0.1:#{port}",
          adapter: :mint,
          pool: false,
          schema: schema
        )

      %{client: client, port: port, schema: schema}
    end

    test "E2E Unary call receiving rich status over HTTP/2 socket", %{client: client} do
      res =
        SimpleGrpc.Client.call(
          client,
          "streaming.StreamingService/UnaryCall",
          %{message: "throw_bad_request", sequence: 1}
        )

      assert {:error, 3, "Validation failed", %RPCStatus{} = rich_status} = res
      assert rich_status.code == 3
      assert length(rich_status.details) == 2

      extracted_details = Status.extract_details(res)
      assert length(extracted_details) == 2
      bad_req = Enum.find(extracted_details, &(&1.__type__ == "google.rpc.BadRequest"))
      assert bad_req != nil
      assert Enum.at(bad_req.field_violations, 0).field == "username"
    end

    test "E2E Unary call receiving rich quota failure over HTTP/2 socket", %{client: client} do
      res =
        SimpleGrpc.Client.call(
          client,
          "streaming.StreamingService/UnaryCall",
          %{message: "throw_quota_exceeded", sequence: 1}
        )

      assert {:error, 8, "Rate limit exceeded", %RPCStatus{} = rich_status} = res
      assert rich_status.code == 8
      assert length(rich_status.details) == 3

      quota = Enum.find(rich_status.details, &(&1.__type__ == "google.rpc.QuotaFailure"))
      assert quota != nil
      assert Enum.at(quota.violations, 0).subject == "client:127.0.0.1"
    end

    test "E2E Unary call receiving plain error without details over HTTP/2 socket", %{
      client: client
    } do
      assert {:error, 5, "Resource not found"} =
               SimpleGrpc.Client.call(
                 client,
                 "streaming.StreamingService/UnaryCall",
                 %{message: "throw_plain_error", sequence: 1}
               )
    end
  end
end
