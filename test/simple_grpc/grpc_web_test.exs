defmodule SimpleGrpc.GrpcWebTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.GrpcWeb

  describe "content-type detection and format negotiation" do
    test "detects binary and text gRPC-Web content types" do
      assert GrpcWeb.grpc_web_request?("application/grpc-web")
      assert GrpcWeb.grpc_web_request?("application/grpc-web+proto")
      assert GrpcWeb.grpc_web_request?("application/grpc-web+json")
      assert GrpcWeb.grpc_web_request?("application/grpc-web-text")
      assert GrpcWeb.grpc_web_request?("application/grpc-web-text+proto")
      assert GrpcWeb.grpc_web_request?("application/grpc-web-text+json")

      refute GrpcWeb.grpc_web_request?("application/grpc")
      refute GrpcWeb.grpc_web_request?("application/json")
      refute GrpcWeb.grpc_web_request?("text/html")
    end

    test "resolves format :binary or :text" do
      assert GrpcWeb.format("application/grpc-web") == :binary
      assert GrpcWeb.format("application/grpc-web+proto") == :binary
      assert GrpcWeb.format("application/grpc-web-text") == :text
      assert GrpcWeb.format("application/grpc-web-text+proto") == :text
      assert GrpcWeb.format("application/grpc") == nil
    end

    test "detects gRPC-Web requests on Plug.Conn via content-type or x-grpc-web header" do
      conn_binary =
        Plug.Test.conn(:post, "/test")
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web+proto")

      conn_text =
        Plug.Test.conn(:post, "/test")
        |> Plug.Conn.put_req_header("content-type", "application/grpc-web-text")

      conn_header =
        Plug.Test.conn(:post, "/test")
        |> Plug.Conn.put_req_header("content-type", "application/grpc")
        |> Plug.Conn.put_req_header("x-grpc-web", "1")

      conn_standard =
        Plug.Test.conn(:post, "/test")
        |> Plug.Conn.put_req_header("content-type", "application/grpc")

      assert GrpcWeb.grpc_web_request?(conn_binary)
      assert GrpcWeb.grpc_web_request?(conn_text)
      assert GrpcWeb.grpc_web_request?(conn_header)
      refute GrpcWeb.grpc_web_request?(conn_standard)
    end

    test "response_content_type negotiation matches requested format" do
      assert GrpcWeb.response_content_type("application/grpc-web-text+proto") ==
               "application/grpc-web-text+proto"

      assert GrpcWeb.response_content_type("application/grpc-web-text") ==
               "application/grpc-web-text"

      assert GrpcWeb.response_content_type("application/grpc-web+proto") ==
               "application/grpc-web+proto"

      assert GrpcWeb.response_content_type("application/grpc-web") ==
               "application/grpc-web"
    end
  end

  describe "trailers-in-body framing" do
    test "frame_trailers with list of key-value tuples" do
      trailers = [{"grpc-status", "0"}, {"grpc-message", "OK"}]
      frame = GrpcWeb.frame_trailers(trailers)

      assert <<128::8, len::32-big, payload::binary-size(len)>> = frame
      assert payload =~ "grpc-status: 0\r\n"
      assert payload =~ "grpc-message: OK\r\n"
    end

    test "frame_trailers with status code, message, and details" do
      frame = GrpcWeb.frame_trailers(:not_found, "User not found")
      assert <<128::8, len::32-big, payload::binary-size(len)>> = frame
      assert payload =~ "grpc-status: 5\r\n"
      assert payload =~ "grpc-message: User%20not%20found\r\n"
    end

    test "parse_trailers_block correctly parses header lines" do
      block = "grpc-status: 0\r\ngrpc-message: OK\r\ncustom-trailer: test-val\r\n"
      trailers = GrpcWeb.parse_trailers_block(block)

      assert trailers == [
               {"grpc-status", "0"},
               {"grpc-message", "OK"},
               {"custom-trailer", "test-val"}
             ]
    end

    test "unframe handles data frames and 0x80 trailer frames" do
      data_payload = "hello world"
      data_frame = <<0::8, byte_size(data_payload)::32-big, data_payload::binary>>
      trailer_frame = GrpcWeb.frame_trailers(0, "OK")
      combined = data_frame <> trailer_frame

      assert {:data, ^data_payload, rest} = GrpcWeb.unframe(combined)
      assert {:trailers, trailers, <<>>} = GrpcWeb.unframe(rest)

      assert Enum.find(trailers, fn {k, v} -> k == "grpc-status" and v == "0" end)
    end
  end

  describe "Base64 text codec" do
    test "encode_response_body and decode_request_body" do
      raw = <<0::8, 5::32-big, "hello">>
      encoded = GrpcWeb.encode_response_body(raw, :text)
      assert is_binary(encoded)
      assert {:ok, decoded} = GrpcWeb.decode_request_body(encoded, :text)
      assert decoded == raw
    end

    test "decode_request_body ignores whitespace and newlines" do
      raw = <<0::8, 5::32-big, "hello">>
      encoded_with_newlines = "\n " <> Base.encode64(raw) <> "\r\n "
      assert {:ok, decoded} = GrpcWeb.decode_request_body(encoded_with_newlines, :text)
      assert decoded == raw
    end
  end

  describe "CORS handling" do
    test "detects OPTIONS CORS preflight" do
      preflight_conn =
        Plug.Test.conn(:options, "/custom_prefix.PrefixService/Echo")
        |> Plug.Conn.put_req_header("origin", "https://example.com")
        |> Plug.Conn.put_req_header("access-control-request-method", "POST")

      normal_conn =
        Plug.Test.conn(:post, "/custom_prefix.PrefixService/Echo")

      assert GrpcWeb.cors_preflight?(preflight_conn)
      refute GrpcWeb.cors_preflight?(normal_conn)
    end

    test "handle_cors_preflight returns 204 with required headers" do
      conn =
        Plug.Test.conn(:options, "/custom_prefix.PrefixService/Echo")
        |> Plug.Conn.put_req_header("origin", "https://example.com")
        |> Plug.Conn.put_req_header("access-control-request-method", "POST")
        |> Plug.Conn.put_req_header("access-control-request-headers", "content-type,x-grpc-web")
        |> GrpcWeb.handle_cors_preflight(allowed_origins: "*")

      assert conn.status == 204
      assert Plug.Conn.get_resp_header(conn, "access-control-allow-origin") == ["*"]
      assert Plug.Conn.get_resp_header(conn, "access-control-allow-methods") == ["POST, OPTIONS"]

      exposed = Plug.Conn.get_resp_header(conn, "access-control-expose-headers") |> List.first()
      assert exposed =~ "grpc-status"
      assert exposed =~ "grpc-message"
    end
  end
end
