defmodule SimpleGrpc.PlugGrpcWebTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.{GrpcWeb, Framing, Protobuf, Schema}

  defmodule WebTestServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/custom_prefix/custom_prefix_service.proto"

    def echo(%{message: "error_not_found"}, _stream) do
      {:error, :not_found, "Requested item not found"}
    end

    def echo(%{message: "error_rich_status"}, _stream) do
      status =
        SimpleGrpc.Status.new(
          :invalid_argument,
          "Invalid input payload",
          [
            SimpleGrpc.ErrorDetails.bad_request([{"message", "message field cannot be empty"}])
          ]
        )

      {:error, status}
    end

    def echo(%{message: msg}, _stream) do
      {:ok, %{reply: "Web: " <> msg}}
    end
  end

  defmodule WebStreamingServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/streaming/streaming_service.proto",
      service: "StreamingService"

    def server_streaming_call(%{message: msg, sequence: seq}, _stream) do
      1..3
      |> Enum.map(fn idx ->
        %{
          result: "Chunk #{idx}: #{msg}",
          sequence: seq + idx,
          is_final: idx == 3
        }
      end)
    end

    def unary_call(r, _),
      do: {:ok, %{result: "StreamWeb: " <> r.message, sequence: r.sequence, is_final: true}}

    def client_streaming_call(r, _), do: r
    def bidirectional_streaming_call(r, _), do: r
  end

  setup_all do
    {:ok, schema_prefix} =
      Schema.from_file("test/fixtures/protos/custom_prefix/custom_prefix_service.proto")

    {:ok, schema_streaming} =
      Schema.from_file("test/fixtures/protos/streaming/streaming_service.proto")

    # Start live Bandit server for network HTTP testing
    {:ok, bandit_pid} =
      Bandit.start_link(
        plug:
          {SimpleGrpc.Plug.GrpcWeb,
           services: [WebTestServer, WebStreamingServer], reflection: true},
        port: 0,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(bandit_pid)

    on_exit(fn ->
      if Process.alive?(bandit_pid), do: Process.exit(bandit_pid, :shutdown)
    end)

    %{
      schema_prefix: schema_prefix,
      schema_streaming: schema_streaming,
      port: port
    }
  end

  describe "Unary gRPC-Web binary calls (application/grpc-web and application/grpc-web+proto)" do
    test "successful unary call returns framed data and 0x80 trailer frame", %{
      schema_prefix: schema
    } do
      req_bytes = Protobuf.encode!("EchoRequest", %{message: "Hello Binary"}, schema)
      framed_req = Framing.frame(req_bytes)

      conn =
        Plug.Test.conn(:post, "/custom_prefix.PrefixService/Echo", framed_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web+proto")
        |> SimpleGrpc.Plug.call(SimpleGrpc.Plug.init(services: [WebTestServer]))

      assert conn.status == 200
      assert Plug.Conn.get_resp_header(conn, "content-type") == ["application/grpc-web+proto"]
      assert Plug.Conn.get_resp_header(conn, "access-control-allow-origin") == ["*"]

      # Unframe response body: must contain payload frame then trailer frame
      assert {:data, resp_payload, rest} = GrpcWeb.unframe(conn.resp_body)
      {:ok, decoded} = Protobuf.decode("EchoResponse", resp_payload, schema)
      assert decoded.reply == "Web: Hello Binary"

      assert {:trailers, trailers, <<>>} = GrpcWeb.unframe(rest)
      assert Enum.find(trailers, fn {k, v} -> k == "grpc-status" and v == "0" end)
    end

    test "error unary call returns 0x80 trailer frame with error status", %{schema_prefix: schema} do
      req_bytes = Protobuf.encode!("EchoRequest", %{message: "error_not_found"}, schema)
      framed_req = Framing.frame(req_bytes)

      conn =
        Plug.Test.conn(:post, "/custom_prefix.PrefixService/Echo", framed_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web")
        |> SimpleGrpc.Plug.call(SimpleGrpc.Plug.init(services: [WebTestServer]))

      assert conn.status == 200
      assert {:trailers, trailers, <<>>} = GrpcWeb.unframe(conn.resp_body)
      assert Enum.find(trailers, fn {k, v} -> k == "grpc-status" and v == "5" end)
      assert Enum.find(trailers, fn {k, v} -> k == "grpc-message" and v =~ "Requested%20item" end)
    end

    test "rich status details are packed into grpc-status-details-bin trailer in body", %{
      schema_prefix: schema
    } do
      req_bytes = Protobuf.encode!("EchoRequest", %{message: "error_rich_status"}, schema)
      framed_req = Framing.frame(req_bytes)

      conn =
        Plug.Test.conn(:post, "/custom_prefix.PrefixService/Echo", framed_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web+proto")
        |> SimpleGrpc.Plug.call(SimpleGrpc.Plug.init(services: [WebTestServer]))

      assert conn.status == 200
      assert {:trailers, trailers, <<>>} = GrpcWeb.unframe(conn.resp_body)

      {code, msg, rich_status} = Framing.extract_grpc_status_details(trailers)
      assert code == 3
      assert msg =~ "Invalid input payload"
      assert is_struct(rich_status, SimpleGrpc.RPC.Status)
      assert length(rich_status.details) == 1
    end
  end

  describe "Unary gRPC-Web Base64 text calls (application/grpc-web-text and application/grpc-web-text+proto)" do
    test "successful Base64 unary call decodes request and encodes response", %{
      schema_prefix: schema
    } do
      req_bytes = Protobuf.encode!("EchoRequest", %{message: "Hello Base64"}, schema)
      framed_req = Framing.frame(req_bytes)
      base64_req = Base.encode64(framed_req)

      conn =
        Plug.Test.conn(:post, "/custom_prefix.PrefixService/Echo", base64_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web-text+proto")
        |> SimpleGrpc.Plug.call(SimpleGrpc.Plug.init(services: [WebTestServer]))

      assert conn.status == 200

      assert Plug.Conn.get_resp_header(conn, "content-type") == [
               "application/grpc-web-text+proto"
             ]

      # Response body should be Base64 text
      {:ok, raw_resp_binary} = Base.decode64(conn.resp_body, ignore: :whitespace)

      assert {:data, resp_payload, rest} = GrpcWeb.unframe(raw_resp_binary)
      {:ok, decoded} = Protobuf.decode("EchoResponse", resp_payload, schema)
      assert decoded.reply == "Web: Hello Base64"

      assert {:trailers, trailers, <<>>} = GrpcWeb.unframe(rest)
      assert Enum.find(trailers, fn {k, v} -> k == "grpc-status" and v == "0" end)
    end

    test "Base64 unary error returns Base64 encoded trailer frame", %{schema_prefix: schema} do
      req_bytes = Protobuf.encode!("EchoRequest", %{message: "error_not_found"}, schema)
      base64_req = Base.encode64(Framing.frame(req_bytes))

      conn =
        Plug.Test.conn(:post, "/custom_prefix.PrefixService/Echo", base64_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web-text")
        |> SimpleGrpc.Plug.call(SimpleGrpc.Plug.init(services: [WebTestServer]))

      assert conn.status == 200
      {:ok, raw_body} = Base.decode64(conn.resp_body, ignore: :whitespace)
      assert {:trailers, trailers, <<>>} = GrpcWeb.unframe(raw_body)
      assert Enum.find(trailers, fn {k, v} -> k == "grpc-status" and v == "5" end)
    end
  end

  describe "Server Streaming gRPC-Web" do
    test "streams chunks followed by final 0x80 trailer frame in binary format", %{
      schema_streaming: schema
    } do
      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "StreamPing", sequence: 100}, schema)

      framed_req = Framing.frame(req_bytes)

      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/ServerStreamingCall", framed_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web+proto")
        |> SimpleGrpc.Plug.call(SimpleGrpc.Plug.init(services: [WebStreamingServer]))

      assert conn.status == 200
      assert Plug.Conn.get_resp_header(conn, "content-type") == ["application/grpc-web+proto"]

      # Unframe 3 data chunks + 1 trailer frame
      assert {:data, chunk1_raw, rest1} = GrpcWeb.unframe(conn.resp_body)
      {:ok, c1} = Protobuf.decode("StreamResponse", chunk1_raw, schema)
      assert c1.result == "Chunk 1: StreamPing"
      assert c1.sequence == 101

      assert {:data, chunk2_raw, rest2} = GrpcWeb.unframe(rest1)
      {:ok, c2} = Protobuf.decode("StreamResponse", chunk2_raw, schema)
      assert c2.result == "Chunk 2: StreamPing"
      assert c2.sequence == 102

      assert {:data, chunk3_raw, rest3} = GrpcWeb.unframe(rest2)
      {:ok, c3} = Protobuf.decode("StreamResponse", chunk3_raw, schema)
      assert c3.result == "Chunk 3: StreamPing"
      assert c3.sequence == 103

      assert {:trailers, trailers, <<>>} = GrpcWeb.unframe(rest3)
      assert Enum.find(trailers, fn {k, v} -> k == "grpc-status" and v == "0" end)
    end

    test "streams Base64 encoded chunks in text format", %{schema_streaming: schema} do
      req_bytes =
        Protobuf.encode!("StreamRequest", %{message: "Base64Stream", sequence: 200}, schema)

      base64_req = Base.encode64(Framing.frame(req_bytes))

      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/ServerStreamingCall", base64_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web-text+proto")
        |> SimpleGrpc.Plug.call(SimpleGrpc.Plug.init(services: [WebStreamingServer]))

      assert conn.status == 200

      assert Plug.Conn.get_resp_header(conn, "content-type") == [
               "application/grpc-web-text+proto"
             ]

      {:ok, raw_body} = Base.decode64(conn.resp_body, ignore: :whitespace)
      assert {:data, chunk1_raw, rest1} = GrpcWeb.unframe(raw_body)
      {:ok, c1} = Protobuf.decode("StreamResponse", chunk1_raw, schema)
      assert c1.result == "Chunk 1: Base64Stream"

      assert {:data, _chunk2, rest2} = GrpcWeb.unframe(rest1)
      assert {:data, _chunk3, rest3} = GrpcWeb.unframe(rest2)
      assert {:trailers, trailers, <<>>} = GrpcWeb.unframe(rest3)
      assert Enum.find(trailers, fn {k, v} -> k == "grpc-status" and v == "0" end)
    end
  end

  describe "SimpleGrpc.Plug.GrpcWeb standalone & CORS configuration" do
    test "handles CORS OPTIONS preflight" do
      conn =
        Plug.Test.conn(:options, "/custom_prefix.PrefixService/Echo")
        |> Plug.Conn.put_req_header("origin", "http://localhost:3000")
        |> Plug.Conn.put_req_header("access-control-request-method", "POST")
        |> Plug.Conn.put_req_header("access-control-request-headers", "content-type,x-grpc-web")
        |> SimpleGrpc.Plug.GrpcWeb.call(
          SimpleGrpc.Plug.GrpcWeb.init(
            services: [WebTestServer],
            allowed_origins: ["http://localhost:3000"]
          )
        )

      assert conn.status == 204

      assert Plug.Conn.get_resp_header(conn, "access-control-allow-origin") == [
               "http://localhost:3000"
             ]

      assert Plug.Conn.get_resp_header(conn, "access-control-allow-methods") == ["POST, OPTIONS"]

      assert Plug.Conn.get_resp_header(conn, "access-control-allow-headers") == [
               "content-type,x-grpc-web"
             ]
    end

    test "unknown method with gRPC-Web returns 200 with trailer frame 12 (UNIMPLEMENTED)" do
      conn =
        Plug.Test.conn(:post, "/NonExistentService/NonExistentMethod", <<>>)
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web+proto")
        |> SimpleGrpc.Plug.GrpcWeb.call(SimpleGrpc.Plug.GrpcWeb.init(services: [WebTestServer]))

      assert conn.status == 200
      assert {:trailers, trailers, <<>>} = GrpcWeb.unframe(conn.resp_body)
      assert Enum.find(trailers, fn {k, v} -> k == "grpc-status" and v == "12" end)
    end
  end

  describe "Live HTTP / TCP Integration over Bandit with SimpleGrpc.Client" do
    test "SimpleGrpc.Client.Adapters.Plug executes gRPC-Web unary calls cleanly", %{
      schema_prefix: schema
    } do
      {:ok, conn} =
        SimpleGrpc.Client.connect({SimpleGrpc.Plug, services: [WebTestServer]}, adapter: :plug)

      req_bytes = Protobuf.encode!("EchoRequest", %{message: "ClientAdapterWeb"}, schema)

      {:ok, resp_bytes, _headers, _} =
        SimpleGrpc.Client.send_unary(conn, "/custom_prefix.PrefixService/Echo", req_bytes,
          headers: [{"content-type", "application/grpc-web+proto"}]
        )

      {:ok, decoded} = Protobuf.decode("EchoResponse", resp_bytes, schema)
      assert decoded.reply == "Web: ClientAdapterWeb"
    end

    test "SimpleGrpc.Client.Adapters.Plug executes Base64 text calls cleanly", %{
      schema_prefix: schema
    } do
      {:ok, conn} =
        SimpleGrpc.Client.connect({SimpleGrpc.Plug, services: [WebTestServer]}, adapter: :plug)

      req_bytes = Protobuf.encode!("EchoRequest", %{message: "ClientAdapterText"}, schema)

      {:ok, resp_bytes, _headers, _} =
        SimpleGrpc.Client.send_unary(conn, "/custom_prefix.PrefixService/Echo", req_bytes,
          headers: [{"content-type", "application/grpc-web-text+proto"}]
        )

      {:ok, decoded} = Protobuf.decode("EchoResponse", resp_bytes, schema)
      assert decoded.reply == "Web: ClientAdapterText"
    end
  end
end
