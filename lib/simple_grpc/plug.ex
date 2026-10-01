defmodule SimpleGrpc.Plug do
  @moduledoc """
  A Plug that serves gRPC requests over HTTP/2 using `SimpleGrpc.Registry`
  and `SimpleGrpc.Protobuf`.
  """

  import Plug.Conn

  alias SimpleGrpc.{Registry, Dispatcher, Protobuf}

  @behaviour Plug

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    path = conn.request_path

    case Registry.lookup_route(path) do
      {:ok, route} ->
        handle_grpc_request(conn, route)

      :not_found ->
        # Drain request body if any, then send trailers-only UNIMPLEMENTED (12)
        with {:ok, _body, conn} <- read_all_body(conn, <<>>) do
          send_trailers_only(conn, 12, "unknown service or method: #{path}")
        else
          _ ->
            send_trailers_only(conn, 12, "unknown service or method: #{path}")
        end
    end
  end

  defp handle_grpc_request(conn, route) do
    with {:ok, body, conn} <- read_all_body(conn, <<>>),
         {:ok, payload_bytes} <- unframe_grpc_message(body),
         {:ok, req_map} <- Protobuf.decode(route.input_message, payload_bytes, route.schema),
         {:ok, resp_data} <- Dispatcher.dispatch(route, req_map, conn),
         {:ok, resp_bytes} <- Protobuf.encode(route.output_message, resp_data, route.schema) do
      framed_response = frame_grpc_message(resp_bytes)
      send_grpc_response(conn, framed_response, 0, "")
    else
      {:error, status_code, message} when is_integer(status_code) ->
        send_trailers_only(conn, status_code, to_string(message))

      {:error, reason} ->
        send_trailers_only(conn, 2, inspect(reason))
    end
  rescue
    e in ArgumentError ->
      send_trailers_only(conn, 3, Exception.message(e))

    e ->
      send_trailers_only(conn, 2, Exception.message(e))
  catch
    :throw, reason ->
      send_trailers_only(conn, 2, inspect(reason))

    :exit, reason ->
      send_trailers_only(conn, 2, inspect(reason))
  end

  defp send_grpc_response(conn, framed_response, status_code, status_message) do
    trailers = [
      {"grpc-status", to_string(status_code)},
      {"grpc-message", to_string(status_message)}
    ]

    case conn.adapter do
      {Bandit.Adapter,
       %Bandit.Adapter{
         transport: %Bandit.HTTP2.Stream{connection_pid: conn_pid, stream_id: stream_id}
       }} ->
        conn =
          conn
          |> put_resp_header("content-type", "application/grpc")
          |> put_resp_header("grpc-accept-encoding", "identity")
          |> send_chunked(200)

        {:ok, conn} = chunk(conn, framed_response)

        # Send trailing headers (trailers) with END_STREAM flag on HTTP/2
        send(conn_pid, {{:send_headers, trailers, true}, stream_id})
        conn

      _other ->
        conn
        |> put_resp_header("content-type", "application/grpc")
        |> put_resp_header("grpc-status", to_string(status_code))
        |> put_resp_header("grpc-message", to_string(status_message))
        |> send_resp(200, framed_response)
    end
  end

  defp send_trailers_only(conn, status_code, status_message) do
    headers = [
      {"content-type", "application/grpc"},
      {"grpc-status", to_string(status_code)},
      {"grpc-message", to_string(status_message)}
    ]

    case conn.adapter do
      {Bandit.Adapter,
       %Bandit.Adapter{
         transport: %Bandit.HTTP2.Stream{connection_pid: conn_pid, stream_id: stream_id}
       }} ->
        send(conn_pid, {{:send_headers, headers, true}, stream_id})
        conn

      _other ->
        conn
        |> put_resp_header("content-type", "application/grpc")
        |> put_resp_header("grpc-status", to_string(status_code))
        |> put_resp_header("grpc-message", to_string(status_message))
        |> send_resp(200, "")
    end
  end

  defp read_all_body(conn, acc) do
    case read_body(conn, length: 10_000_000) do
      {:ok, body, conn} ->
        {:ok, acc <> body, conn}

      {:more, body, conn} ->
        read_all_body(conn, acc <> body)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Unframes a 5-byte length-prefixed gRPC message.
  """
  def unframe_grpc_message(
        <<_compressed::8, length::32-big, data::binary-size(length), _rest::binary>>
      ) do
    {:ok, data}
  end

  def unframe_grpc_message(<<>>) do
    {:ok, <<>>}
  end

  def unframe_grpc_message(invalid) do
    {:error, "Invalid gRPC frame header: #{inspect(invalid)}"}
  end

  @doc """
  Frames a binary payload with the standard 5-byte gRPC prefix.
  """
  def frame_grpc_message(payload) when is_binary(payload) do
    <<0::8, byte_size(payload)::32-big, payload::binary>>
  end
end
