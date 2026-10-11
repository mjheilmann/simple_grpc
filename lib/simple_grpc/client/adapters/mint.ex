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
          keepalive_time = Keyword.get(opts, :keepalive_time)
          keepalive_timeout = Keyword.get(opts, :keepalive_timeout, 10_000)
          permit_without_calls = Keyword.get(opts, :permit_without_calls, false)

          timer = schedule_keepalive(keepalive_time)

          {:ok,
           %{
             conn: conn,
             target: target,
             opts: opts,
             scheme: scheme,
             host: host,
             port: port,
             streams: %{},
             requests: %{},
             keepalive_time: keepalive_time,
             keepalive_timeout: keepalive_timeout,
             permit_without_calls: permit_without_calls,
             keepalive_timer: timer,
             pending_pings: %{}
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    end

    @impl true
    def handle_call({:ping, payload, timeout}, from, state) do
      if is_struct(state.conn, Mint.HTTP2) do
        ping_payload = payload || :crypto.strong_rand_bytes(8)

        case Mint.HTTP2.ping(state.conn, ping_payload) do
          {:ok, next_conn, ref} ->
            timer_ref = Process.send_after(self(), {:ping_timeout, ref}, timeout)

            new_pings =
              Map.put(
                state.pending_pings,
                ref,
                {from, System.monotonic_time(:microsecond), timer_ref}
              )

            {:noreply, %{state | conn: next_conn, pending_pings: new_pings}}

          {:error, next_conn, reason} ->
            {:reply, {:error, reason}, %{state | conn: next_conn}}
        end
      else
        {:reply, {:error, :not_http2}, state}
      end
    end

    @impl true
    def handle_call({:send_unary, path, payload, opts}, from, state) do
      timeout = Keyword.get(opts, :timeout, @default_timeout)
      custom_headers = Keyword.get(opts, :headers, [])
      framed_payload = Framing.frame(payload, opts)

      headers = [
        {"content-type", "application/grpc"},
        {"te", "trailers"}
        | format_headers(custom_headers)
      ]

      req_meta = %{
        from: from,
        buffer: <<>>,
        status: 200,
        headers: [],
        trailers: [],
        timeout: timeout
      }

      payload_size = byte_size(framed_payload)

      if payload_size <= 16384 do
        with {:ok, conn, request_ref} <-
               Mint.HTTP.request(state.conn, "POST", path, headers, framed_payload) do
          new_requests = Map.put(state.requests, request_ref, req_meta)
          {:noreply, %{state | conn: conn, requests: new_requests}}
        else
          {:error, next_conn, reason} ->
            {:reply, {:error, 2, inspect(reason)}, %{state | conn: next_conn}}

          {:error, reason} ->
            {:reply, {:error, 2, inspect(reason)}, state}
        end
      else
        with {:ok, conn, request_ref} <-
               Mint.HTTP.request(state.conn, "POST", path, headers, :stream),
             {:ok, conn, early_responses} <-
               stream_large_body(conn, request_ref, framed_payload, timeout, []) do
          new_requests = Map.put(state.requests, request_ref, req_meta)

          {new_requests, new_streams, new_pings} =
            process_multiplexed_responses(
              early_responses,
              new_requests,
              state.streams,
              state.pending_pings
            )

          {:noreply,
           %{
             state
             | conn: conn,
               requests: new_requests,
               streams: new_streams,
               pending_pings: new_pings
           }}
        else
          {:error, next_conn, reason} ->
            {:reply, {:error, 2, inspect(reason)}, %{state | conn: next_conn}}

          {:error, reason} ->
            {:reply, {:error, 2, inspect(reason)}, state}
        end
      end
    end

    @impl true
    def handle_call({:start_stream, path, request_data, opts}, _from, state) do
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
             Mint.HTTP.request(state.conn, "POST", path, headers, :stream),
           {:ok, conn, early_responses} <-
             (if is_stream_req do
                case stream_enumerable_body(conn, request_ref, request_data, timeout, opts) do
                  {:ok, next_conn, early} ->
                    case Mint.HTTP.stream_request_body(next_conn, request_ref, :eof) do
                      {:ok, final_conn} -> {:ok, final_conn, early}
                      {:error, final_conn, err} -> {:error, final_conn, err}
                    end

                  {:error, next_conn, err} ->
                    {:error, next_conn, err}
                end
              else
                framed = Framing.frame(request_data, opts)
                stream_large_body(conn, request_ref, framed, timeout, [])
              end) do
        # Initialize stream buffer in state and route any early responses
        stream_meta = %{buffer: <<>>, trailers: [], done: false, headers: []}
        new_streams = Map.put(state.streams, request_ref, stream_meta)
        new_streams = route_async_responses(early_responses, new_streams)

        {:reply, {:ok, request_ref}, %{state | conn: conn, streams: new_streams}}
      else
        {:error, next_conn, reason} ->
          {:reply, {:error, 2, inspect(reason)}, %{state | conn: next_conn}}

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

        {:error, next_conn, new_streams, status, msg, rich_status} ->
          {:reply, {:error, status, msg, rich_status},
           %{state | conn: next_conn, streams: new_streams}}

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
    def handle_info(:keepalive_tick, state) do
      has_active_calls = map_size(state.requests) > 0 or map_size(state.streams) > 0

      should_ping =
        (has_active_calls or state.permit_without_calls) and is_struct(state.conn, Mint.HTTP2)

      if should_ping do
        case Mint.HTTP2.ping(state.conn) do
          {:ok, next_conn, ref} ->
            timer_ref = Process.send_after(self(), {:ping_timeout, ref}, state.keepalive_timeout)

            new_pings =
              Map.put(
                state.pending_pings,
                ref,
                {nil, System.monotonic_time(:microsecond), timer_ref}
              )

            timer = schedule_keepalive(state.keepalive_time)

            {:noreply,
             %{state | conn: next_conn, pending_pings: new_pings, keepalive_timer: timer}}

          {:error, next_conn, reason} ->
            {:stop, {:keepalive_error, reason}, %{state | conn: next_conn}}
        end
      else
        timer = schedule_keepalive(state.keepalive_time)
        {:noreply, %{state | keepalive_timer: timer}}
      end
    end

    @impl true
    def handle_info({:ping_timeout, ref}, state) do
      case Map.get(state.pending_pings, ref) do
        {from, _sent_at, _t_ref} ->
          if from, do: GenServer.reply(from, {:error, :timeout})
          {:stop, :keepalive_timeout, state}

        nil ->
          {:noreply, state}
      end
    end

    @impl true
    def handle_info(message, state) do
      case Mint.HTTP.stream(state.conn, message) do
        {:ok, conn, responses} ->
          # Route any responses into active unary requests, streaming buffers, and pings
          {new_requests, new_streams, new_pings} =
            process_multiplexed_responses(
              responses,
              state.requests,
              state.streams,
              state.pending_pings
            )

          {:noreply,
           %{
             state
             | conn: conn,
               requests: new_requests,
               streams: new_streams,
               pending_pings: new_pings
           }}

        {:error, conn, _reason, _responses} ->
          {:noreply, %{state | conn: conn}}

        :unknown ->
          {:noreply, state}
      end
    end

    defp process_multiplexed_responses(responses, requests, streams, pending_pings) do
      Enum.reduce(responses, {requests, streams, pending_pings}, fn resp,
                                                                    {req_acc, str_acc, ping_acc} ->
        case resp do
          {:pong, ref} ->
            case Map.get(ping_acc, ref) do
              {from, start_us, timer_ref} ->
                if timer_ref, do: Process.cancel_timer(timer_ref)
                latency_us = System.monotonic_time(:microsecond) - start_us
                if from, do: GenServer.reply(from, {:ok, latency_us})
                {req_acc, str_acc, Map.delete(ping_acc, ref)}

              nil ->
                {req_acc, str_acc, ping_acc}
            end

          {:status, ref, status} ->
            if Map.has_key?(req_acc, ref) do
              {update_in(req_acc, [ref, :status], fn _ -> status end), str_acc, ping_acc}
            else
              {req_acc, route_async_responses([resp], str_acc), ping_acc}
            end

          {:headers, ref, headers} ->
            if Map.has_key?(req_acc, ref) do
              {update_in(req_acc, [ref, :headers], &(&1 ++ headers)), str_acc, ping_acc}
            else
              {req_acc, route_async_responses([resp], str_acc), ping_acc}
            end

          {:data, ref, chunk} ->
            if Map.has_key?(req_acc, ref) do
              {update_in(req_acc, [ref, :buffer], &(&1 <> chunk)), str_acc, ping_acc}
            else
              {req_acc, route_async_responses([resp], str_acc), ping_acc}
            end

          {:trailers, ref, trailers} ->
            if Map.has_key?(req_acc, ref) do
              {update_in(req_acc, [ref, :trailers], &(&1 ++ trailers)), str_acc, ping_acc}
            else
              {req_acc, route_async_responses([resp], str_acc), ping_acc}
            end

          {:done, ref} ->
            if Map.has_key?(req_acc, ref) do
              req_meta = Map.get(req_acc, ref)
              all_headers = req_meta.headers ++ req_meta.trailers

              {grpc_status, grpc_msg, rich_status} =
                Framing.extract_grpc_status_details(all_headers)

              resp_encoding = extract_encoding(all_headers)

              res =
                if grpc_status == 0 and req_meta.status == 200 do
                  case Framing.unframe(req_meta.buffer, encoding: resp_encoding) do
                    {:ok, response_payload, _rest} ->
                      {:ok, response_payload, req_meta.headers, req_meta.trailers}

                    {:incomplete, <<>>} ->
                      {:ok, <<>>, req_meta.headers, req_meta.trailers}

                    {:incomplete, _} ->
                      {:ok, req_meta.buffer, req_meta.headers, req_meta.trailers}

                    {:error, reason} ->
                      {:error, 2, reason}
                  end
                else
                  if rich_status do
                    {:error, grpc_status, grpc_msg, rich_status}
                  else
                    {:error, grpc_status, grpc_msg}
                  end
                end

              GenServer.reply(req_meta.from, res)
              {Map.delete(req_acc, ref), str_acc, ping_acc}
            else
              {req_acc, route_async_responses([resp], str_acc), ping_acc}
            end

          {:error, ref, reason} ->
            if Map.has_key?(req_acc, ref) do
              req_meta = Map.get(req_acc, ref)
              GenServer.reply(req_meta.from, {:error, 2, inspect(reason)})
              {Map.delete(req_acc, ref), str_acc, ping_acc}
            else
              {req_acc, route_async_responses([resp], str_acc), ping_acc}
            end

          other ->
            {req_acc, route_async_responses([other], str_acc), ping_acc}
        end
      end)
    end

    defp schedule_keepalive(interval) when is_integer(interval) and interval > 0 do
      Process.send_after(self(), :keepalive_tick, interval)
    end

    defp schedule_keepalive(_), do: nil

    defp stream_enumerable_body(conn, request_ref, enumerable, timeout, opts) do
      Enum.reduce_while(enumerable, {:ok, conn, []}, fn item, {:ok, acc_conn, early_responses} ->
        framed = Framing.frame(item, opts)

        case stream_single_framed_chunk(acc_conn, request_ref, framed, timeout, early_responses) do
          {:ok, next_conn, new_early} -> {:cont, {:ok, next_conn, new_early}}
          {:error, next_conn, reason} -> {:halt, {:error, next_conn, reason}}
        end
      end)
    end

    defp stream_single_framed_chunk(conn, _request_ref, <<>>, _timeout, early_responses) do
      {:ok, conn, early_responses}
    end

    defp stream_single_framed_chunk(conn, request_ref, data, timeout, early_responses)
         when is_binary(data) do
      if is_struct(conn, Mint.HTTP2) and not Map.has_key?(conn.ref_to_stream_id, request_ref) do
        {:ok, conn, early_responses}
      else
        window =
          if is_struct(conn, Mint.HTTP2) do
            cw = Mint.HTTP2.get_window_size(conn, :connection)
            rw = Mint.HTTP2.get_window_size(conn, {:request, request_ref})
            max(min(cw, rw), 0)
          else
            byte_size(data)
          end

        if window > 0 do
          chunk_len = min(min(window, 16384), byte_size(data))
          <<chunk::binary-size(^chunk_len), rest::binary>> = data

          case Mint.HTTP.stream_request_body(conn, request_ref, chunk) do
            {:ok, next_conn} ->
              stream_single_framed_chunk(next_conn, request_ref, rest, timeout, early_responses)

            {:error, next_conn, reason} ->
              {:error, next_conn, reason}
          end
        else
          receive do
            message ->
              case Mint.HTTP.stream(conn, message) do
                {:ok, next_conn, responses} ->
                  stream_single_framed_chunk(
                    next_conn,
                    request_ref,
                    data,
                    timeout,
                    early_responses ++ responses
                  )

                {:error, next_conn, reason, _responses} ->
                  {:error, next_conn, reason}

                :unknown ->
                  stream_single_framed_chunk(conn, request_ref, data, timeout, early_responses)
              end
          after
            timeout ->
              {:error, conn, :timeout}
          end
        end
      end
    end

    defp stream_large_body(conn, request_ref, <<>>, _timeout, early_responses) do
      case Mint.HTTP.stream_request_body(conn, request_ref, :eof) do
        {:ok, next_conn} -> {:ok, next_conn, early_responses}
        {:error, next_conn, reason} -> {:error, next_conn, reason}
      end
    end

    defp stream_large_body(conn, request_ref, data, timeout, early_responses)
         when is_binary(data) do
      if is_struct(conn, Mint.HTTP2) and not Map.has_key?(conn.ref_to_stream_id, request_ref) do
        {:ok, conn, early_responses}
      else
        window =
          if is_struct(conn, Mint.HTTP2) do
            cw = Mint.HTTP2.get_window_size(conn, :connection)
            rw = Mint.HTTP2.get_window_size(conn, {:request, request_ref})
            max(min(cw, rw), 0)
          else
            byte_size(data)
          end

        if window > 0 do
          chunk_len = min(min(window, 16384), byte_size(data))
          <<chunk::binary-size(^chunk_len), rest::binary>> = data

          case Mint.HTTP.stream_request_body(conn, request_ref, chunk) do
            {:ok, next_conn} ->
              stream_large_body(next_conn, request_ref, rest, timeout, early_responses)

            {:error, next_conn, reason} ->
              {:error, next_conn, reason}
          end
        else
          receive do
            message ->
              case Mint.HTTP.stream(conn, message) do
                {:ok, next_conn, responses} ->
                  stream_large_body(
                    next_conn,
                    request_ref,
                    data,
                    timeout,
                    early_responses ++ responses
                  )

                {:error, next_conn, reason, _responses} ->
                  {:error, next_conn, reason}

                :unknown ->
                  stream_large_body(conn, request_ref, data, timeout, early_responses)
              end
          after
            timeout ->
              {:error, conn, :timeout}
          end
        end
      end
    end

    # -------------------------------------------------------------------------
    # Next Message Receiver for Progressive Streams
    # -------------------------------------------------------------------------

    defp receive_next_message_loop(conn, streams, request_ref, timeout) do
      stream_meta =
        Map.get(streams, request_ref, %{buffer: <<>>, trailers: [], done: false, headers: []})

      resp_encoding = extract_encoding(stream_meta.headers ++ stream_meta.trailers)

      case Framing.unframe(stream_meta.buffer, encoding: resp_encoding) do
        {:ok, message, rest_buffer} ->
          updated_meta = %{stream_meta | buffer: rest_buffer}
          new_streams = Map.put(streams, request_ref, updated_meta)
          {:ok, conn, new_streams, message}

        {:incomplete, _} ->
          if stream_meta.done do
            all_headers = stream_meta.trailers ++ stream_meta.headers
            {status, msg, rich_status} = Framing.extract_grpc_status_details(all_headers)

            new_streams = Map.delete(streams, request_ref)

            if status == 0 do
              {:done, conn, new_streams}
            else
              if rich_status do
                {:error, conn, new_streams, status, msg, rich_status}
              else
                {:error, conn, new_streams, status, msg}
              end
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

    defp extract_encoding(headers) do
      Enum.find_value(headers, "gzip", fn {k, v} ->
        if String.downcase(to_string(k)) == "grpc-encoding", do: to_string(v)
      end)
    end

    defp route_async_responses(responses, streams) do
      Enum.reduce(responses, streams, fn
        {:headers, ref, headers}, acc ->
          case Map.get(acc, ref) do
            nil ->
              acc

            meta ->
              Map.put(acc, ref, %{
                meta
                | headers: meta.headers ++ headers,
                  trailers: meta.trailers ++ headers
              })
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
    use_pool = Keyword.get(opts, :pool, true)

    if use_pool do
      case SimpleGrpc.Client.Pool.start_link(target, opts) do
        {:ok, pool_pid} ->
          {:ok, {:pool, pool_pid}}

        {:error, reason} ->
          {:error, reason}
      end
    else
      case Connection.start_link(target, opts) do
        {:ok, pid} ->
          {:ok, {:conn, pid}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def send_unary({:pool, pool_pid}, path, payload, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    SimpleGrpc.Client.Pool.checkout(
      pool_pid,
      fn conn_pid ->
        send_unary_conn(conn_pid, path, payload, opts)
      end,
      timeout + 2000
    )
  end

  def send_unary({:conn, conn_pid}, path, payload, opts) do
    send_unary_conn(conn_pid, path, payload, opts)
  end

  def send_unary(pid, path, payload, opts) when is_pid(pid) do
    send_unary_conn(pid, path, payload, opts)
  end

  defp send_unary_conn(pid, path, payload, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    GenServer.call(pid, {:send_unary, path, payload, opts}, timeout + 2000)
  end

  @impl true
  def send_stream({:pool, pool_pid}, path, request_data, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    SimpleGrpc.Client.Pool.checkout(
      pool_pid,
      fn conn_pid ->
        send_stream_conn(conn_pid, path, request_data, opts)
      end,
      timeout + 2000
    )
  end

  def send_stream({:conn, conn_pid}, path, request_data, opts) do
    send_stream_conn(conn_pid, path, request_data, opts)
  end

  def send_stream(pid, path, request_data, opts) when is_pid(pid) do
    send_stream_conn(pid, path, request_data, opts)
  end

  defp send_stream_conn(pid, path, request_data, opts) do
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
  def close({:pool, pool_pid}) do
    SimpleGrpc.Client.Pool.close(pool_pid)
  end

  def close({:conn, conn_pid}) do
    close_conn(conn_pid)
  end

  def close(pid) when is_pid(pid) do
    close_conn(pid)
  end

  def close(_), do: :ok

  @impl true
  def ping({:pool, pool_pid}, opts) do
    SimpleGrpc.Client.Pool.ping(pool_pid, opts)
  end

  def ping({:conn, conn_pid}, opts) do
    ping_conn(conn_pid, opts)
  end

  def ping(pid, opts) when is_pid(pid) do
    ping_conn(pid, opts)
  end

  defp ping_conn(pid, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    payload = Keyword.get(opts, :payload)
    GenServer.call(pid, {:ping, payload, timeout}, timeout + 2000)
  end

  defp close_conn(pid) do
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
end
