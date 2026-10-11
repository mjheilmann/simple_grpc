defmodule SimpleGrpc.CompressionTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.{Compression, Framing}

  defmodule CompressionServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    def unary_call(req, _stream) do
      {:ok, %{result: "Hello, " <> req.message, sequence: req.sequence + 1, is_final: true}}
    end

    def server_streaming_call(req, _stream) do
      ["Hi", "Hello", "Welcome"]
      |> Stream.map(fn prefix ->
        %{result: "#{prefix}, #{req.message}!", sequence: req.sequence, is_final: false}
      end)
    end

    def client_streaming_call(req_stream, _stream) do
      names =
        req_stream
        |> Enum.map(& &1.message)
        |> Enum.join(", ")

      {:ok, %{result: "Hello all: #{names}", sequence: 10, is_final: true}}
    end

    def bidirectional_streaming_call(req_stream, _stream) do
      Stream.map(req_stream, fn req ->
        %{result: "Echo: " <> req.message, sequence: req.sequence, is_final: false}
      end)
    end
  end

  defmodule CompressionClient do
    use SimpleGrpc.Client,
      client_file: "priv/protos/streaming/streaming_service.proto",
      adapter: SimpleGrpc.Client.Adapters.Plug
  end

  # ===========================================================================
  # 1. SimpleGrpc.Compression Engine Unit Tests
  # ===========================================================================

  describe "SimpleGrpc.Compression algorithms and header support" do
    test "supported_encodings/0 and supported_encoding?/1" do
      assert "identity" in Compression.supported_encodings()
      assert "gzip" in Compression.supported_encodings()
      assert "deflate" in Compression.supported_encodings()
      assert "zstd" in Compression.supported_encodings()

      assert Compression.supported_encoding?("gzip")
      assert Compression.supported_encoding?("GZIP")
      assert Compression.supported_encoding?(:gzip)
      assert Compression.supported_encoding?("deflate")
      assert Compression.supported_encoding?(:deflate)
      assert Compression.supported_encoding?("zstd")
      assert Compression.supported_encoding?(:zstd)
      assert Compression.supported_encoding?("identity")
      assert Compression.supported_encoding?(:identity)

      refute Compression.supported_encoding?("snappy")
      refute Compression.supported_encoding?("bzip2")
      refute Compression.supported_encoding?(nil)
      refute Compression.supported_encoding?("")
    end

    test "accept_encoding_header/0 returns default accepted encodings" do
      header = Compression.accept_encoding_header()
      assert String.contains?(header, "gzip")
      assert String.contains?(header, "deflate")
      assert String.contains?(header, "identity")
      assert String.contains?(header, "zstd")
    end

    test "compress and decompress with identity" do
      data = "The quick brown fox jumps over the lazy dog"
      assert {:ok, compressed, 0} = Compression.compress(data, "identity")
      assert compressed == data

      assert {:ok, decompressed} = Compression.decompress(compressed, "identity")
      assert decompressed == data
    end

    test "compress and decompress with gzip" do
      data = String.duplicate("SimpleGrpc High Performance Compression ", 50)
      assert {:ok, compressed, 1} = Compression.compress(data, "gzip")
      assert byte_size(compressed) < byte_size(data)

      assert {:ok, decompressed} = Compression.decompress(compressed, "gzip")
      assert decompressed == data
    end

    test "compress and decompress with deflate" do
      data = String.duplicate("SimpleGrpc Deflate Payload Data ", 50)
      assert {:ok, compressed, 1} = Compression.compress(data, "deflate")
      assert byte_size(compressed) < byte_size(data)

      assert {:ok, decompressed} = Compression.decompress(compressed, "deflate")
      assert decompressed == data
    end

    test "compress and decompress with zstd" do
      data = String.duplicate("SimpleGrpc Zstandard Payload Data ", 50)
      assert {:ok, compressed, 1} = Compression.compress(data, "zstd")

      assert {:ok, decompressed} = Compression.decompress(compressed, "zstd")
      assert decompressed == data
    end

    test "compress with unsupported encoding returns error" do
      assert {:error, msg} = Compression.compress("data", "lz4")
      assert String.contains?(msg, "Unsupported compression algorithm")
    end

    test "decompress with corrupted payload returns error" do
      assert {:error, msg} = Compression.decompress("not-gzip-data", "gzip")
      assert String.contains?(msg, "Failed to decompress")

      assert {:error, msg2} = Compression.decompress("not-deflate-data", "deflate")
      assert String.contains?(msg2, "Failed to decompress")
    end

    test "negotiate/2 selects best supported encoding" do
      assert Compression.negotiate(nil) == "identity"
      assert Compression.negotiate("") == "identity"
      assert Compression.negotiate("identity") == "identity"
      assert Compression.negotiate("gzip, deflate") == "gzip"
      assert Compression.negotiate("deflate, identity") == "deflate"
      assert Compression.negotiate("gzip, deflate", :deflate) == "deflate"
      assert Compression.negotiate("identity, gzip", :gzip) == "gzip"
      assert Compression.negotiate("unknown, custom") == "identity"
    end
  end

  # ===========================================================================
  # 2. Framing 1-Byte Flag & Encoding Tests
  # ===========================================================================

  describe "SimpleGrpc.Framing with compression flag and encoding" do
    test "frame/2 with compress: true produces flag 1 and gzip body" do
      raw = "Test payload for gRPC frame"
      framed = Framing.frame(raw, compress: true)

      assert <<1::8, len::32-big, compressed::binary-size(len)>> = framed
      assert {:ok, uncompressed} = Compression.decompress(compressed, "gzip")
      assert uncompressed == raw
    end

    test "frame/2 with encoding: :deflate produces flag 1 and deflate body" do
      raw = "Deflate test payload for gRPC frame"
      framed = Framing.frame(raw, encoding: :deflate)

      assert <<1::8, len::32-big, compressed::binary-size(len)>> = framed
      assert {:ok, uncompressed} = Compression.decompress(compressed, "deflate")
      assert uncompressed == raw
    end

    test "frame/2 with encoding: :identity or uncompressed produces flag 0" do
      raw = "Raw payload"
      framed = Framing.frame(raw)
      assert <<0::8, len::32-big, payload::binary-size(len)>> = framed
      assert payload == raw
      assert len == byte_size(raw)

      framed_identity = Framing.frame(raw, encoding: "identity")
      assert <<0::8, ^len::32-big, ^payload::binary-size(^len)>> = framed_identity
    end

    test "unframe/2 decompresses flag 1 according to opts" do
      raw = "Message to unframe"
      gzip_framed = Framing.frame(raw, encoding: "gzip")
      deflate_framed = Framing.frame(raw, encoding: "deflate")
      zstd_framed = Framing.frame(raw, encoding: "zstd")

      assert {:ok, ^raw, <<>>} = Framing.unframe(gzip_framed, encoding: "gzip")
      assert {:ok, ^raw, <<>>} = Framing.unframe(deflate_framed, encoding: "deflate")
      assert {:ok, ^raw, <<>>} = Framing.unframe(zstd_framed, encoding: "zstd")
    end

    test "unframe/2 fails cleanly on corrupted compressed data or invalid flag" do
      corrupted_frame = <<1::8, 4::32-big, "junk">>
      assert {:error, msg} = Framing.unframe(corrupted_frame, encoding: "gzip")
      assert String.contains?(msg, "Failed to decompress")

      invalid_flag_frame = <<2::8, 4::32-big, "test">>
      assert {:error, msg2} = Framing.unframe(invalid_flag_frame)
      assert String.contains?(msg2, "Unsupported compression flag")
    end
  end

  # ===========================================================================
  # 3. Server-Side Compression Negotiation & Rejection Tests (via Plug)
  # ===========================================================================

  describe "Server-side Plug compression handling" do
    setup do
      plug_opts = SimpleGrpc.Plug.init(server: CompressionServer)
      schema = CompressionServer.__schema__()
      %{plug_opts: plug_opts, schema: schema}
    end

    test "rejects unsupported incoming grpc-encoding with gRPC status 12 UNIMPLEMENTED", %{
      plug_opts: plug_opts
    } do
      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", <<0::8, 0::32-big>>)
        |> Plug.Conn.put_req_header("content-type", "application/grpc")
        |> Plug.Conn.put_req_header("grpc-encoding", "snappy")
        |> SimpleGrpc.Plug.call(plug_opts)

      headers = conn.resp_headers
      {status_code, status_msg} = Framing.extract_grpc_status(headers)

      assert status_code == 12
      assert status_msg == "unsupported grpc-encoding: snappy"

      accept_encoding =
        Enum.find_value(headers, fn {k, v} ->
          if k == "grpc-accept-encoding", do: v
        end)

      assert accept_encoding == Compression.accept_encoding_header()
    end

    test "accepts incoming gzip-compressed request and decodes successfully", %{
      plug_opts: plug_opts,
      schema: schema
    } do
      {:ok, req_bytes} =
        SimpleGrpc.Protobuf.encode(
          "streaming.StreamRequest",
          %{message: "GzipUser", sequence: 1},
          schema
        )

      framed_req = Framing.frame(req_bytes, encoding: "gzip")

      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", framed_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc")
        |> Plug.Conn.put_req_header("grpc-encoding", "gzip")
        |> Plug.Conn.put_req_header("grpc-accept-encoding", "identity")
        |> SimpleGrpc.Plug.call(plug_opts)

      {status_code, _} = Framing.extract_grpc_status(conn.resp_headers)
      assert status_code == 0

      {:ok, resp_bytes, <<>>} = Framing.unframe(conn.resp_body)
      {:ok, resp} = SimpleGrpc.Protobuf.decode("streaming.StreamResponse", resp_bytes, schema)
      assert resp.result == "Hello, GzipUser"
    end

    test "accepts incoming deflate-compressed request and decodes successfully", %{
      plug_opts: plug_opts,
      schema: schema
    } do
      {:ok, req_bytes} =
        SimpleGrpc.Protobuf.encode(
          "streaming.StreamRequest",
          %{message: "DeflateUser", sequence: 1},
          schema
        )

      framed_req = Framing.frame(req_bytes, encoding: "deflate")

      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", framed_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc")
        |> Plug.Conn.put_req_header("grpc-encoding", "deflate")
        |> Plug.Conn.put_req_header("grpc-accept-encoding", "identity")
        |> SimpleGrpc.Plug.call(plug_opts)

      {status_code, _} = Framing.extract_grpc_status(conn.resp_headers)
      assert status_code == 0

      {:ok, resp_bytes, <<>>} = Framing.unframe(conn.resp_body)
      {:ok, resp} = SimpleGrpc.Protobuf.decode("streaming.StreamResponse", resp_bytes, schema)
      assert resp.result == "Hello, DeflateUser"
    end

    test "compresses response with gzip when client advertises grpc-accept-encoding: gzip", %{
      plug_opts: plug_opts,
      schema: schema
    } do
      {:ok, req_bytes} =
        SimpleGrpc.Protobuf.encode(
          "streaming.StreamRequest",
          %{message: "Alice", sequence: 1},
          schema
        )

      framed_req = Framing.frame(req_bytes)

      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", framed_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc")
        |> Plug.Conn.put_req_header("grpc-accept-encoding", "gzip, identity")
        |> SimpleGrpc.Plug.call(plug_opts)

      headers = conn.resp_headers
      {status_code, _} = Framing.extract_grpc_status(headers)
      assert status_code == 0

      resp_encoding =
        Enum.find_value(headers, fn {k, v} ->
          if k == "grpc-encoding", do: v
        end)

      assert resp_encoding == "gzip"

      # Verify flag is 1 and response is gzip-compressed
      assert <<1::8, len::32-big, compressed::binary-size(len)>> = conn.resp_body
      assert {:ok, resp_bytes} = Compression.decompress(compressed, "gzip")
      {:ok, resp} = SimpleGrpc.Protobuf.decode("streaming.StreamResponse", resp_bytes, schema)
      assert resp.result == "Hello, Alice"
    end

    test "compresses response with deflate when client advertises grpc-accept-encoding: deflate",
         %{
           plug_opts: plug_opts,
           schema: schema
         } do
      {:ok, req_bytes} =
        SimpleGrpc.Protobuf.encode(
          "streaming.StreamRequest",
          %{message: "Bob", sequence: 1},
          schema
        )

      framed_req = Framing.frame(req_bytes)

      conn =
        Plug.Test.conn(:post, "/streaming.StreamingService/UnaryCall", framed_req)
        |> Plug.Conn.put_req_header("content-type", "application/grpc")
        |> Plug.Conn.put_req_header("grpc-accept-encoding", "deflate, identity")
        |> SimpleGrpc.Plug.call(plug_opts)

      headers = conn.resp_headers
      {status_code, _} = Framing.extract_grpc_status(headers)
      assert status_code == 0

      resp_encoding =
        Enum.find_value(headers, fn {k, v} ->
          if k == "grpc-encoding", do: v
        end)

      assert resp_encoding == "deflate"

      # Verify flag is 1 and response is deflate-compressed
      assert <<1::8, len::32-big, compressed::binary-size(len)>> = conn.resp_body
      assert {:ok, resp_bytes} = Compression.decompress(compressed, "deflate")
      {:ok, resp} = SimpleGrpc.Protobuf.decode("streaming.StreamResponse", resp_bytes, schema)
      assert resp.result == "Hello, Bob"
    end
  end

  # ===========================================================================
  # 4. Client-Side High-Level In-Memory Plug DSL Tests
  # ===========================================================================

  describe "Client High-Level Calls with compression options" do
    setup do
      {:ok, conn} =
        SimpleGrpc.Client.connect({SimpleGrpc.Plug, services: [CompressionServer]},
          adapter: :plug
        )

      %{conn: conn}
    end

    test "unary call with compress: true sends gzip and receives decompressed response", %{
      conn: conn
    } do
      assert {:ok, reply} =
               CompressionClient.unary_call(conn, %{message: "CompressClient", sequence: 1},
                 compress: true
               )

      assert reply.result == "Hello, CompressClient"
    end

    test "unary call with encoding: :deflate sends deflate and receives decompressed response", %{
      conn: conn
    } do
      assert {:ok, reply} =
               CompressionClient.unary_call(conn, %{message: "DeflateClient", sequence: 1},
                 encoding: :deflate
               )

      assert reply.result == "Hello, DeflateClient"
    end

    test "server streaming call receives compressed frames transparently", %{conn: conn} do
      assert {:ok, stream} =
               CompressionClient.server_streaming_call(conn, %{message: "Streamer", sequence: 1},
                 encoding: :gzip
               )

      messages = Enum.map(stream, & &1.result)
      assert messages == ["Hi, Streamer!", "Hello, Streamer!", "Welcome, Streamer!"]
    end

    test "client streaming call sends compressed frames", %{conn: conn} do
      req_stream = [
        %{message: "Alpha", sequence: 1},
        %{message: "Beta", sequence: 2},
        %{message: "Gamma", sequence: 3}
      ]

      assert {:ok, reply} =
               CompressionClient.client_streaming_call(conn, req_stream, encoding: :deflate)

      assert reply.result == "Hello all: Alpha, Beta, Gamma"
    end

    test "bidi streaming call handles bidirectional compressed streams", %{conn: conn} do
      req_stream = [
        %{message: "One", sequence: 1},
        %{message: "Two", sequence: 2}
      ]

      assert {:ok, stream} =
               CompressionClient.bidirectional_streaming_call(conn, req_stream, encoding: :gzip)

      messages = Enum.map(stream, & &1.result)
      assert messages == ["Echo: One", "Echo: Two"]
    end
  end

  # ===========================================================================
  # 5. End-to-End Over Network Socket (Bandit Server + Mint Client)
  # ===========================================================================

  describe "End-to-End Compression over HTTP/2 Mint socket" do
    setup do
      port = 50200 + :erlang.phash2(make_ref(), 1000)

      start_supervised!(
        {Bandit,
         plug: {SimpleGrpc.Plug, services: [CompressionServer]},
         port: port,
         scheme: :http,
         startup_log: false,
         thousand_island_options: [num_acceptors: 2]}
      )

      # Give Bandit server a few milliseconds to bind
      Process.sleep(50)

      schema = CompressionServer.__schema__()

      {:ok, client} =
        SimpleGrpc.Client.connect("http://127.0.0.1:#{port}",
          adapter: :mint,
          pool: false,
          schema: schema
        )

      %{client: client, port: port, schema: schema}
    end

    test "E2E Unary call with gzip compression over HTTP/2 socket", %{
      client: client,
      schema: schema
    } do
      {:ok, reply} =
        SimpleGrpc.Client.call(
          client,
          "streaming.StreamingService/UnaryCall",
          %{message: "NetworkGzip", sequence: 1},
          encoding: :gzip
        )

      assert reply.result == "Hello, NetworkGzip"

      # Also test low-level send_unary to inspect response encoding header
      {:ok, req_bytes} =
        SimpleGrpc.Protobuf.encode(
          "streaming.StreamRequest",
          %{message: "RawGzip", sequence: 1},
          schema
        )

      {:ok, resp_bytes, headers, trailers} =
        SimpleGrpc.Client.send_unary(client, "/streaming.StreamingService/UnaryCall", req_bytes,
          encoding: :gzip
        )

      assert is_binary(resp_bytes)
      all_headers = headers ++ trailers

      resp_encoding =
        Enum.find_value(all_headers, fn {k, v} ->
          if String.downcase(to_string(k)) == "grpc-encoding", do: to_string(v)
        end)

      assert resp_encoding in ["gzip", "deflate"]
    end

    test "E2E Unary call with deflate compression over HTTP/2 socket", %{client: client} do
      {:ok, reply} =
        SimpleGrpc.Client.call(
          client,
          "streaming.StreamingService/UnaryCall",
          %{message: "NetworkDeflate", sequence: 1},
          encoding: :deflate
        )

      assert reply.result == "Hello, NetworkDeflate"
    end

    test "E2E Server streaming with compression over HTTP/2 socket", %{client: client} do
      {:ok, stream} =
        SimpleGrpc.Client.call(
          client,
          "streaming.StreamingService/ServerStreamingCall",
          %{message: "NetworkStream", sequence: 1},
          encoding: :gzip
        )

      messages = Enum.map(stream, & &1.result)

      assert messages == [
               "Hi, NetworkStream!",
               "Hello, NetworkStream!",
               "Welcome, NetworkStream!"
             ]
    end
  end
end
