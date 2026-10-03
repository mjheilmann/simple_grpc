defmodule SimpleGrpc.CompressionTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.Framing
  alias SimpleGrpc.Client

  defmodule CompressionServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    def unary_call(req, _stream) do
      {:ok, %{result: "Echo: " <> req.message, sequence: req.sequence + 1, is_final: true}}
    end

    def server_streaming_call(req, _stream) do
      1..3
      |> Stream.map(fn i ->
        %{
          result: "Chunk #{i}: #{req.message}",
          sequence: req.sequence + i,
          is_final: i == 3
        }
      end)
    end

    def client_streaming_call(req_stream, _stream) do
      count = Enum.count(req_stream)
      {:ok, %{result: "Total: #{count}", sequence: count, is_final: true}}
    end

    def bidirectional_streaming_call(req_stream, _stream) do
      Stream.map(req_stream, fn req ->
        %{result: "Echo: " <> req.message, sequence: req.sequence, is_final: false}
      end)
    end
  end

  defmodule TypedStreamingClient do
    use SimpleGrpc.Client,
      client_file: "priv/protos/streaming/streaming_service.proto"
  end

  setup_all do
    {:ok, sup_pid} = Bandit.start_link(plug: {SimpleGrpc.Plug, services: [CompressionServer]}, port: 0)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup_pid)

    on_exit(fn ->
      if Process.alive?(sup_pid), do: Process.exit(sup_pid, :shutdown)
    end)

    %{port: port}
  end

  describe "Framing Compression" do
    test "frames with compression flag 0 by default and uncompressed payload" do
      payload = "Hello, uncompressed world!"
      framed = Framing.frame(payload)
      assert <<0::8, len::32-big, data::binary>> = framed
      assert len == byte_size(payload)
      assert data == payload

      assert {:ok, unframed, <<>>} = Framing.unframe(framed)
      assert unframed == payload
    end

    test "frames with compression flag 1 when compress: true and gunzips on unframe" do
      payload = String.duplicate("Repeated string with high compressibility!", 50)
      framed = Framing.frame(payload, compress: true)

      assert <<1::8, len::32-big, compressed_data::binary>> = framed
      assert len == byte_size(compressed_data)
      assert byte_size(compressed_data) < byte_size(payload)

      assert {:ok, unframed, <<>>} = Framing.unframe(framed)
      assert unframed == payload
    end

    test "frames with encoding: :gzip and gunzips correctly" do
      payload = "Gzip test payload data"
      framed = Framing.frame(payload, encoding: :gzip)
      assert <<1::8, _len::32-big, _data::binary>> = framed

      assert {:ok, unframed, <<>>} = Framing.unframe(framed)
      assert unframed == payload
    end
  end

  describe "Client and Server Gzip Compression over HTTP/2 (Bandit)" do
    test "sends compressed request and receives response", %{port: port} do
      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      # Request with compress: true option
      assert {:ok, resp} =
               TypedStreamingClient.unary_call(
                 conn,
                 %{message: "CompressedMessage", sequence: 10},
                 compress: true
               )

      assert resp.result == "Echo: CompressedMessage"
      assert resp.sequence == 11
      Client.close(conn)
    end

    test "sends compressed server streaming request", %{port: port} do
      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      assert {:ok, stream} =
               TypedStreamingClient.server_streaming_call(
                 conn,
                 %{message: "StreamCompress", sequence: 100},
                 compress: true
               )

      results = Enum.to_list(stream)
      assert length(results) == 3
      assert Enum.at(results, 0).result == "Chunk 1: StreamCompress"
      assert Enum.at(results, 2).is_final == true
      Client.close(conn)
    end
  end

  describe "In-Memory Plug Transport with Compression" do
    test "sends compressed unary request via Plug adapter" do
      {:ok, conn} = Client.connect({SimpleGrpc.Plug, services: [CompressionServer]}, adapter: :plug)

      assert {:ok, resp} =
               TypedStreamingClient.unary_call(
                 conn,
                 %{message: "PlugCompress", sequence: 50},
                 compress: true
               )

      assert resp.result == "Echo: PlugCompress"
      assert resp.sequence == 51
      Client.close(conn)
    end

    test "rejects request with unsupported grpc-encoding header" do
      {:ok, conn} = Client.connect({SimpleGrpc.Plug, services: [CompressionServer]}, adapter: :plug)

      assert {:error, 12, msg} =
               TypedStreamingClient.unary_call(
                 conn,
                 %{message: "BadEncoding", sequence: 1},
                 headers: [{"grpc-encoding", "snappy"}]
               )

      assert msg =~ "unsupported grpc-encoding"
      Client.close(conn)
    end
  end
end
