defmodule SimpleGrpc.Client.Adapters.Mint do
  @moduledoc """
  Pure-Elixir HTTP/2 network transport adapter using `Mint.HTTP2`.
  Supports multiplexed unary and streaming gRPC over HTTP/2 plaintext and TLS
  with connection state management and request isolation.
  """

  @behaviour SimpleGrpc.Client.Adapter

  alias SimpleGrpc.Client.Framing

  @default_timeout 5_000

  defmodule Connection do
    @moduledoc false
    use GenServer

    alias SimpleGrpc.Client.Framing

    @default_timeout 5_000

    def start_link(target, opts) do
      GenServer.start_link(__MODULE__, {target, opts})
    end

    @impl true
    def init({target, opts}) do
      {scheme, host, port} = parse_target(target, opts)
      protocols = Keyword.get(opts, :protocols, [:http2])
      transport_opts = Keyword.get(opts, :transport_opts, [])
      mint_opts = [protocols: protocols, transport_opts: transport_opts]

      case Mint.HTTP.connect(scheme, host, port, mint_opts) do
        {:ok, conn} ->
          {:ok, %{conn: conn, target: target, opts: opts, scheme: scheme, host: host, port: port}}

        {:error, reason} ->
          {:stop, reason}
      end
    end

    @impl true
    def handle_call({:send_unary, path, payload, opts}, _from, state) do
      timeout = Keyword.get(opts, :timeout, @default_timeout)
      custom_headers = Keyword.get(opts, :headers, [])
      framed_payload = Framing.frame(payload)

      headers = [
        {"content-type", "application/grpc"},
        {"te", "trailers"}
        | format_headers(custom_headers)
      ]

      with {:ok, conn, request_ref} <-
             Mint.HTTP.request(state.conn, "POST", path, headers, framed_payload),
           {:ok, next_conn, status, resp_headers, resp_data, trailers} <-
             receive_response(conn, request_ref, timeout, <<>>, [], []) do
        {grpc_status, grpc_msg} =
          Framing.extract_grpc_status(trailers ++ resp_headers)

        res =
          if grpc_status == 0 and status == 200 do
            case Framing.unframe(resp_data) do
              {:ok, response_payload, _rest} ->
                {:ok, response_payload, resp_headers, trailers}

              {:incomplete, <<>>} ->
                {:ok, <<>>, resp_headers, trailers}

              {:incomplete, _} ->
                {:ok, resp_data, resp_headers, trailers}
            end
          else
            {:error, grpc_status, grpc_msg}
          end

        {:reply, res, %{state | conn: next_conn}}
      else
        {:error, next_conn, reason} ->
          {:reply, {:error, 2, inspect(reason)}, %{state | conn: next_conn}}

        {:error, reason} ->
          {:reply, {:error, 2, inspect(reason)}, state}
      end
    end

    @impl true
    def handle_call({:send_stream, path, request_data, opts}, _from, state) do
      timeout = Keyword.get(opts, :timeout, @default_timeout)
      custom_headers = Keyword.get(opts, :headers, [])

      headers = [
        {"content-type", "application/grpc"},
        {"te", "trailers"}
        | format_headers(custom_headers)
      ]

      is_stream_req =
        is_struct(request_data, Stream) or is_list(request_data) or is_function(request_data, 2)

      with {:ok, conn, request_ref} <-
             (if is_stream_req do
                Mint.HTTP.request(state.conn, "POST", path, headers, :stream)
              else
                Mint.HTTP.request(state.conn, "POST", path, headers, Framing.frame(request_data))
              end) do
        conn =
          if is_stream_req do
            conn =
              Enum.reduce(request_data, conn, fn chunk, acc_conn ->
                framed = Framing.frame(chunk)
                case Mint.HTTP.stream_request_body(acc_conn, request_ref, framed) do
                  {:ok, next_conn} -> next_conn
                  _ -> acc_conn
                end
              end)

            case Mint.HTTP.stream_request_body(conn, request_ref, :eof) do
              {:ok, next_conn} -> next_conn
              _ -> conn
            end
          else
            conn
          end

        case receive_response(conn, request_ref, timeout, <<>>, [], []) do
          {:ok, next_conn, status, resp_headers, resp_data, trailers} ->
            {grpc_status, grpc_msg} =
              Framing.extract_grpc_status(trailers ++ resp_headers)

            res =
              if grpc_status == 0 and status == 200 do
                stream =
                  Stream.unfold(resp_data, fn
                    <<>> ->
                      nil

                    binary when is_binary(binary) ->
                      case Framing.unframe(binary) do
                        {:ok, payload, rest} -> {payload, rest}
                        _ -> nil
                      end
                  end)

                {:ok, stream, resp_headers, trailers}
              else
                {:error, grpc_status, grpc_msg}
              end

            {:reply, res, %{state | conn: next_conn}}

          {:error, next_conn, reason} ->
            {:reply, {:error, 2, inspect(reason)}, %{state | conn: next_conn}}
        end
      else
        {:error, reason} ->
          {:reply, {:error, 2, inspect(reason)}, state}
      end
    end

    @impl true
    def handle_call(:close, _from, state) do
      if state.conn do
        Mint.HTTP.close(state.conn)
      end

      {:stop, :normal, :ok, %{state | conn: nil}}
    end

    @impl true
    def handle_info(message, state) do
      case Mint.HTTP.stream(state.conn, message) do
        {:ok, conn, _responses} ->
          {:noreply, %{state | conn: conn}}

        {:error, conn, _reason, _responses} ->
          {:noreply, %{state | conn: conn}}

        :unknown ->
          {:noreply, state}
      end
    end

    # -------------------------------------------------------------------------
    # Response receiver loop
    # -------------------------------------------------------------------------

    defp receive_response(conn, request_ref, timeout, acc_data, acc_headers, acc_trailers) do
      receive do
        message ->
          case Mint.HTTP.stream(conn, message) do
            {:ok, conn, responses} ->
              handle_stream_responses(
                conn,
                request_ref,
                timeout,
                responses,
                acc_data,
                acc_headers,
                acc_trailers
              )

            {:error, conn, reason, _responses} ->
              {:error, conn, reason}

            :unknown ->
              receive_response(
                conn,
                request_ref,
                timeout,
                acc_data,
                acc_headers,
                acc_trailers
              )
          end
      after
        timeout ->
          {:error, conn, :timeout}
      end
    end

    defp handle_stream_responses(
           conn,
           request_ref,
           timeout,
           responses,
           acc_data,
           acc_headers,
           acc_trailers
         ) do
      Enum.reduce_while(
        responses,
        {:cont, {conn, acc_data, acc_headers, acc_trailers}},
        fn
          {:status, ^request_ref, status}, {:cont, {c, d, h, t}} ->
            {:cont, {:cont, {c, d, [{:status, status} | h], t}}}

          {:headers, ^request_ref, headers}, {:cont, {c, d, h, t}} ->
            {:cont, {:cont, {c, d, h ++ headers, t}}}

          {:data, ^request_ref, chunk}, {:cont, {c, d, h, t}} ->
            {:cont, {:cont, {c, d <> chunk, h, t}}}

          {:trailers, ^request_ref, trailers}, {:cont, {c, d, h, t}} ->
            {:cont, {:cont, {c, d, h, t ++ trailers}}}

          {:done, ^request_ref}, {:cont, {c, d, h, t}} ->
            status =
              Enum.find_value(h, 200, fn
                {:status, s} -> s
                _ -> nil
              end)

            {:halt, {:ok, c, status, h, d, t}}

          {:error, ^request_ref, reason}, _acc ->
            {:halt, {:error, conn, reason}}

          _other, acc ->
            {:cont, acc}
        end
      )
      |> case do
        {:ok, c, status, h, d, t} ->
          {:ok, c, status, h, d, t}

        {:error, c, reason} ->
          {:error, c, reason}

        {:cont, {c, d, h, t}} ->
          receive_response(c, request_ref, timeout, d, h, t)
      end
    end

    defp parse_target(target, opts) do
      scheme = Keyword.get(opts, :scheme, :http)

      cond do
        is_tuple(target) and tuple_size(target) == 3 ->
          target

        is_tuple(target) and tuple_size(target) == 2 ->
          {host, port} = target
          {scheme, host, port}

        is_binary(target) and String.starts_with?(target, ["http://", "https://"]) ->
          uri = URI.parse(target)
          s = if uri.scheme == "https", do: :https, else: :http
          {s, uri.host, uri.port || if(s == :https, do: 443, else: 80)}

        is_binary(target) and String.contains?(target, ":") ->
          [host, port_str] = String.split(target, ":", parts: 2)
          {scheme, host, String.to_integer(port_str)}

        is_binary(target) ->
          {scheme, target, if(scheme == :https, do: 443, else: 80)}

        true ->
          {scheme, "127.0.0.1", 50051}
      end
    end

    defp format_headers(headers) do
      Enum.map(headers, fn {k, v} -> {to_string(k), to_string(v)} end)
    end
  end

  # ===========================================================================
  # SimpleGrpc.Client.Adapter Behaviour Implementation
  # ===========================================================================

  @impl true
  def connect(target, opts \\ []) do
    case Connection.start_link(target, opts) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def send_unary(pid, path, payload, opts) when is_pid(pid) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    GenServer.call(pid, {:send_unary, path, payload, opts}, timeout + 2000)
  end

  @impl true
  def send_stream(pid, path, request_data, opts) when is_pid(pid) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    GenServer.call(pid, {:send_stream, path, request_data, opts}, timeout + 2000)
  end

  @impl true
  def close(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.call(pid, :close, 2000)
      catch
        :exit, _ -> :ok
      end
    else
      :ok
    end
  end

  def close(_), do: :ok
end
