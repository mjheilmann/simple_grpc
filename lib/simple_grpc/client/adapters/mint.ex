defmodule SimpleGrpc.Client.Adapters.Mint do
  @moduledoc """
  Pure-Elixir HTTP/2 network transport adapter using `Mint.HTTP2`.
  Supports multiplexed unary and streaming gRPC over HTTP/2 plaintext and TLS
  with connection state management, progressive request streaming, and lazy response streaming.
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
          {:ok,
           %{
             conn: conn,
             target: target,
             opts: opts,
             scheme: scheme,
             host: host,
             port: port,
             streams: %{}
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    end

    @impl true
    def handle_call({:send_unary, path, payload, opts}, _from, state) do
      timeout = Keyword.get(opts, :timeout, @default_timeout)
      custom_headers = Keyword.get(opts, :headers, [])
      framed_payload = Framing.frame(payload, opts)

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
    def handle_call({:start_stream, path, request_data, opts}, _from, state) do
      _timeout = Keyword.get(opts, :timeout, @default_timeout)
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
                Mint.HTTP.request(state.conn, "POST", path, headers, Framing.frame(request_data, opts))
              end) do
        # Stream request chunks progressively
        conn =
          if is_stream_req do
            conn =
              Enum.reduce(request_data, conn, fn chunk, acc_conn ->
                framed = Framing.frame(chunk, opts)
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

        # Initialize stream buffer in state
        stream_meta = %{buffer: <<>>, trailers: [], done: false, headers: []}
        new_streams = Map.put(state.streams, request_ref, stream_meta)

        {:reply, {:ok, request_ref}, %{state | conn: conn, streams: new_streams}}
      else
        {:error, reason} ->
          {:reply, {:error, 2, inspect(reason)}, state}
      end
    end

    @impl true
    def handle_call({:next_stream_message, request_ref, timeout}, _from, state) do
      case receive_next_message_loop(state.conn, state.streams, request_ref, timeout) do
        {:ok, next_conn, new_streams, message} ->
          {:reply, {:ok, message}, %{state | conn: next_conn, streams: new_streams}}

        {:done, next_conn, new_streams} ->
          {:reply, :done, %{state | conn: next_conn, streams: new_streams}}

        {:error, next_conn, new_streams, status, msg} ->
          {:reply, {:error, status, msg}, %{state | conn: next_conn, streams: new_streams}}

        {:error, next_conn, new_streams, reason} ->
          {:reply, {:error, 2, inspect(reason)}, %{state | conn: next_conn, streams: new_streams}}
      end
    end

    @impl true
    def handle_call(:close, _from, state) do
      if state.conn do
        Mint.HTTP.close(state.conn)
      end

      {:stop, :normal, :ok, %{state | conn: nil, streams: %{}}}
    end

    @impl true
    def handle_cast({:cancel_stream, request_ref}, state) do
      conn =
        if state.conn && is_struct(state.conn, Mint.HTTP2) do
          case Mint.HTTP2.cancel_request(state.conn, request_ref) do
            {:ok, next_conn} -> next_conn
            _ -> state.conn
          end
        else
          state.conn
        end

      new_streams = Map.delete(state.streams, request_ref)
      {:noreply, %{state | conn: conn, streams: new_streams}}
    end

    @impl true
    def handle_info(message, state) do
      case Mint.HTTP.stream(state.conn, message) do
        {:ok, conn, responses} ->
          # Route any chunk responses into active stream buffers
          new_streams = route_async_responses(responses, state.streams)
          {:noreply, %{state | conn: conn, streams: new_streams}}

        {:error, conn, _reason, _responses} ->
          {:noreply, %{state | conn: conn}}

        :unknown ->
          {:noreply, state}
      end
    end

    # -------------------------------------------------------------------------
    # Response receiver loop for Unary calls
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

    # -------------------------------------------------------------------------
    # Next Message Receiver for Progressive Streams
    # -------------------------------------------------------------------------

    defp receive_next_message_loop(conn, streams, request_ref, timeout) do
      stream_meta = Map.get(streams, request_ref, %{buffer: <<>>, trailers: [], done: false, headers: []})

      case Framing.unframe(stream_meta.buffer) do
        {:ok, message, rest_buffer} ->
          updated_meta = %{stream_meta | buffer: rest_buffer}
          new_streams = Map.put(streams, request_ref, updated_meta)
          {:ok, conn, new_streams, message}

        {:incomplete, _} ->
          if stream_meta.done do
            {status, msg} = Framing.extract_grpc_status(stream_meta.trailers ++ stream_meta.headers)
            new_streams = Map.delete(streams, request_ref)

            if status == 0 do
              {:done, conn, new_streams}
            else
              {:error, conn, new_streams, status, msg}
            end
          else
            receive do
              message ->
                case Mint.HTTP.stream(conn, message) do
                  {:ok, next_conn, responses} ->
                    new_streams = route_async_responses(responses, streams)
                    receive_next_message_loop(next_conn, new_streams, request_ref, timeout)

                  {:error, next_conn, reason, _responses} ->
                    new_streams = Map.delete(streams, request_ref)
                    {:error, next_conn, new_streams, reason}

                  :unknown ->
                    receive_next_message_loop(conn, streams, request_ref, timeout)
                end
            after
              timeout ->
                {:error, conn, streams, :timeout}
            end
          end
      end
    end

    defp route_async_responses(responses, streams) do
      Enum.reduce(responses, streams, fn
        {:headers, ref, headers}, acc ->
          case Map.get(acc, ref) do
            nil -> acc
            meta -> Map.put(acc, ref, %{meta | headers: meta.headers ++ headers, trailers: meta.trailers ++ headers})
          end

        {:data, ref, chunk}, acc ->
          case Map.get(acc, ref) do
            nil -> acc
            meta -> Map.put(acc, ref, %{meta | buffer: meta.buffer <> chunk})
          end

        {:trailers, ref, trailers}, acc ->
          case Map.get(acc, ref) do
            nil -> acc
            meta -> Map.put(acc, ref, %{meta | trailers: meta.trailers ++ trailers})
          end

        {:done, ref}, acc ->
          case Map.get(acc, ref) do
            nil -> acc
            meta -> Map.put(acc, ref, %{meta | done: true})
          end

        _, acc ->
          acc
      end)
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

    case GenServer.call(pid, {:start_stream, path, request_data, opts}, timeout + 2000) do
      {:ok, request_ref} ->
        stream =
          Stream.resource(
            fn -> {pid, request_ref} end,
            fn {p, ref} ->
              case GenServer.call(p, {:next_stream_message, ref, timeout}, timeout + 2000) do
                {:ok, message} ->
                  {[message], {p, ref}}

                :done ->
                  {:halt, {p, ref}}

                {:error, status, msg} ->
                  raise RuntimeError, "gRPC error #{status}: #{msg}"

                {:error, reason} ->
                  raise RuntimeError, "gRPC stream error: #{inspect(reason)}"
              end
            end,
            fn {p, ref} ->
              if Process.alive?(p) do
                GenServer.cast(p, {:cancel_stream, ref})
              end
            end
          )

        {:ok, stream, [], []}

      {:error, code, msg} ->
        {:error, code, msg}

      {:error, reason} ->
        {:error, 2, inspect(reason)}
    end
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
