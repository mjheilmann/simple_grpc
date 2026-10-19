defmodule SimpleGrpc.Http2FramingTest do
  @moduledoc """
  Checks the exact HTTP/2 frames Bandit sends for a gRPC response.

  Client libraries differ in how strictly they treat a frame that does not fit the stream state:
  curl, Go and Mint ignore a DATA frame arriving after the stream was closed, gun treats it as a
  connection error and drops the connection (and the caller then waits for a reconnect). Reading
  raw frames pins the behaviour independent of any one client.
  """
  use ExUnit.Case, async: false

  import Bitwise

  alias SimpleGrpc.Protobuf

  @preface "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

  defmodule Alpha do
    use SimpleGrpc.Server,
      files: ["test/fixtures/protos/two_services/two_services.proto"],
      service: "Alpha"

    def echo(%{text: "BIG"}, _ctx), do: {:ok, %{text: String.duplicate("x", 200_000)}}
    def echo(req, _ctx), do: {:ok, req}
    def only_alpha(req, _ctx), do: {:ok, req}

    def count(%{text: "FAIL"}, _ctx) do
      Stream.map(1..3, fn
        3 -> raise "mid-stream"
        i -> %{text: "item#{i}"}
      end)
    end

    def count(req, _ctx), do: Stream.map(1..3, &%{text: "#{req.text}#{&1}"})
    def boom(_req, _ctx), do: raise("boom")
  end

  defmodule Pipeline do
    use Plug.Builder

    plug(SimpleGrpc.Plug.Service, server: SimpleGrpc.Http2FramingTest.Alpha)
    plug(SimpleGrpc.Plug.NotFound)
  end

  setup_all do
    {:ok, sup} = Bandit.start_link(plug: Pipeline, port: 0, startup_log: false)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup)
    on_exit(fn -> if Process.alive?(sup), do: Process.exit(sup, :shutdown) end)
    %{port: port, schema: Alpha.__schema__()}
  end

  test "unary: HEADERS, DATA, trailers with END_STREAM, and nothing after", ctx do
    frames = exchange(ctx, "/twosvc.Alpha/Echo", "hello")

    assert [{:headers, 0}, {:data, 0}, {:headers, :end_stream}] = summary(frames)
    assert trailer_status(frames) == "0"
  end

  test "large unary response still ends with the trailers", ctx do
    frames = exchange(ctx, "/twosvc.Alpha/Echo", "BIG")

    kinds = frames |> summary() |> Enum.map(&elem(&1, 0))
    assert List.first(kinds) == :headers
    assert Enum.at(kinds, -1) == :headers
    assert Enum.all?(Enum.slice(kinds, 1..-2//1), &(&1 == :data))
    assert {:headers, :end_stream} = List.last(summary(frames))
    assert trailer_status(frames) == "0"
  end

  test "server streaming: all messages, then the trailers, and nothing after", ctx do
    frames = exchange(ctx, "/twosvc.Alpha/Count", "n")
    kinds = summary(frames)

    assert {:headers, 0} = List.first(kinds)
    assert {:headers, :end_stream} = List.last(kinds)
    assert Enum.all?(Enum.slice(kinds, 1..-2//1), &match?({:data, _}, &1))
    assert trailer_status(frames) == "0"
  end

  test "a failure part-way through a stream ends it with plain trailers", ctx do
    frames = exchange(ctx, "/twosvc.Alpha/Count", "FAIL")
    kinds = summary(frames)

    assert {:headers, 0} = List.first(kinds)
    assert {:headers, :end_stream} = List.last(kinds)
    assert [{:data, 0}, {:data, 0}] = Enum.slice(kinds, 1..-2//1)
    assert trailer_status(frames) == "13"

    {:headers, _, trailers} = frames |> Enum.filter(&match?({:headers, _, _}, &1)) |> List.last()
    refute Enum.any?(trailers, fn {k, _} -> String.starts_with?(k, ":") end)
  end

  test "an error status is one HEADERS frame with END_STREAM", ctx do
    for {path, status} <- [
          {"/twosvc.Alpha/Boom", "13"},
          {"/twosvc.Alpha/Nope", "12"},
          {"/twosvc.Missing/Echo", "12"}
        ] do
      frames = exchange(ctx, path, "x")

      assert [{:headers, :end_stream}] = summary(frames), "#{path}: #{inspect(summary(frames))}"
      assert trailer_status(frames) == status
    end
  end

  test "a second request on the same connection is answered", ctx do
    {:ok, sock} = connect(ctx.port)
    {first, table} = request(sock, 1, "/twosvc.Alpha/Echo", body(ctx, "a"))
    {second, _table} = request(sock, 3, "/twosvc.Alpha/Echo", body(ctx, "b"), table)
    :gen_tcp.close(sock)

    assert [{:headers, 0}, {:data, 0}, {:headers, :end_stream}] = summary(first)
    assert [{:headers, 0}, {:data, 0}, {:headers, :end_stream}] = summary(second)
  end

  # -- helpers ---------------------------------------------------------------------------------

  defp exchange(ctx, path, text) do
    {:ok, sock} = connect(ctx.port)
    {frames, _table} = request(sock, 1, path, body(ctx, text))
    :gen_tcp.close(sock)
    frames
  end

  defp body(ctx, text) do
    bytes = IO.iodata_to_binary(Protobuf.encode!("twosvc.Msg", %{text: text}, ctx.schema))
    <<0, byte_size(bytes)::32, bytes::binary>>
  end

  defp connect(port) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, nodelay: true])
    :ok = :gen_tcp.send(sock, @preface <> frame(4, 0, 0, <<>>))
    {:ok, sock}
  end

  # Sends one request on `stream_id`, reads until that stream ends, then waits a little longer for
  # any frame that should not be there. Returns the frames seen on the stream.
  defp request(sock, stream_id, path, grpc_body, table \\ HPAX.new(4096)) do
    {block, _} =
      HPAX.encode(
        Enum.map(
          [
            {":method", "POST"},
            {":scheme", "http"},
            {":path", path},
            {":authority", "localhost"},
            {"content-type", "application/grpc"},
            {"te", "trailers"}
          ],
          fn {k, v} -> {:no_store, k, v} end
        ),
        HPAX.new(4096)
      )

    :ok = :gen_tcp.send(sock, frame(1, 0x4, stream_id, IO.iodata_to_binary(block)))
    :ok = :gen_tcp.send(sock, frame(0, 0x1, stream_id, grpc_body))
    read_stream(sock, stream_id, [], false, table)
  end

  defp read_stream(sock, stream_id, acc, ended, table) do
    timeout = if ended, do: 300, else: 5000

    case read_frame(sock, timeout) do
      {:ok, {type, flags, ^stream_id, payload}} when type in [0, 1, 3] ->
        ends = (flags &&& 0x1) == 0x1
        acc = [{type, flags, payload} | acc]
        if type == 0 and payload != <<>>, do: replenish(sock, stream_id, byte_size(payload))
        read_stream(sock, stream_id, acc, ended or ends, table)

      {:ok, _other_stream_or_control} ->
        read_stream(sock, stream_id, acc, ended, table)

      :timeout when ended ->
        decode(Enum.reverse(acc), table)

      :timeout ->
        flunk("timed out; frames so far: #{inspect(elem(decode(Enum.reverse(acc), table), 0))}")

      :closed ->
        decode(Enum.reverse(acc), table)
    end
  end

  defp replenish(sock, stream_id, n) do
    :gen_tcp.send(sock, frame(8, 0, 0, <<n::32>>) <> frame(8, 0, stream_id, <<n::32>>))
  end

  defp read_frame(sock, timeout) do
    with {:ok, <<len::24, type, flags, _::1, id::31>>} <- :gen_tcp.recv(sock, 9, timeout),
         {:ok, payload} <- recv_payload(sock, len, timeout) do
      {:ok, {type, flags, id, payload}}
    else
      {:error, :timeout} -> :timeout
      {:error, _} -> :closed
    end
  end

  defp recv_payload(_sock, 0, _timeout), do: {:ok, <<>>}
  defp recv_payload(sock, len, timeout), do: :gen_tcp.recv(sock, len, timeout)

  defp frame(type, flags, stream_id, payload) do
    <<byte_size(payload)::24, type, flags, 0::1, stream_id::31, payload::binary>>
  end

  # frames as {kind, flags, payload}; DATA is 0, HEADERS 1, RST_STREAM 3
  defp decode(frames, table) do
    Enum.map_reduce(frames, table, fn
      {1, flags, payload}, table ->
        {:ok, headers, table} = HPAX.decode(payload, table)
        {{:headers, flags, headers}, table}

      {0, flags, payload}, table ->
        {{:data, flags, payload}, table}

      {3, _flags, <<code::32>>}, table ->
        {{:rst_stream, 0, code}, table}
    end)
  end

  defp summary(frames) do
    frames
    |> Enum.reject(&match?({:rst_stream, _, 0}, &1))
    |> Enum.map(fn
      {:rst_stream, _, code} -> {:rst_stream, code}
      {kind, flags, _} -> {kind, if((flags &&& 0x1) == 0x1, do: :end_stream, else: 0)}
    end)
  end

  defp trailer_status(frames) do
    {:headers, _, headers} = frames |> Enum.filter(&match?({:headers, _, _}, &1)) |> List.last()
    Enum.find_value(headers, fn {k, v} -> if k == "grpc-status", do: v end)
  end
end
