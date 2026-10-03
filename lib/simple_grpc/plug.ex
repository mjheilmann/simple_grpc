defmodule SimpleGrpc.Plug do
  @moduledoc """
  A Plug that serves gRPC requests over HTTP/2 using `SimpleGrpc.Registry`
  and `SimpleGrpc.Protobuf`.
  """

  import Plug.Conn

  alias SimpleGrpc.{Registry, Dispatcher, Protobuf}

  @behaviour Plug

  @impl true
  def init(opts) do
    opts_map = Enum.into(opts, %{})
    services = Map.get(opts_map, :services)
    reflection_opt = Map.get(opts_map, :reflection, false)
    unmatched = Map.get(opts_map, :unmatched, :unimplemented)

    routes =
      if services do
        base_routes = build_service_routes(services)

        if reflection_opt do
          schemas = extract_schemas_from_services(services)
          refl_opts = normalize_reflection_opts(reflection_opt, schemas, services)
          refl_routes = SimpleGrpc.Reflection.Server.build_reflection_routes(refl_opts)
          Map.merge(base_routes, refl_routes)
        else
          base_routes
        end
      else
        nil
      end

    %{
      routes: routes,
      unmatched: unmatched
    }
  end

  @impl true
  def call(conn, state) do
    start_time = System.monotonic_time()
    path = conn.request_path
    state = normalize_state(state)

    route_lookup =
      if state.routes do
        case Map.get(state.routes, path) do
          nil -> :not_found
          route -> {:ok, route}
        end
      else
        Registry.lookup_route(path)
      end

    case route_lookup do
      {:ok, route} ->
        emit_telemetry_start(conn, path, route, start_time)
        handle_grpc_request(conn, route, start_time)

      :not_found ->
        if state.unmatched == :pass do
          conn
        else
          emit_telemetry_start(conn, path, nil, start_time)
          emit_telemetry_stop(conn, path, nil, nil, nil, 12, "unknown service or method: #{path}", start_time)
          send_trailers_only(conn, 12, "unknown service or method: #{path}")
        end
    end
  end

  defp normalize_state(%{unmatched: _} = state), do: state
  defp normalize_state(opts) when is_list(opts), do: init(opts)
  defp normalize_state(_), do: %{routes: nil, unmatched: :unimplemented}

  defp build_service_routes(services) when is_list(services) do
    Enum.reduce(services, %{}, fn item, acc ->
      case item do
        mod when is_atom(mod) ->
          if function_exported?(mod, :__schema__, 0) do
            schema = mod.__schema__()
            {:ok, routes} = Registry.generate_routes(schema, mod)
            Map.merge(acc, routes)
          else
            acc
          end

        %SimpleGrpc.Schema{} = schema ->
          {:ok, routes} = Registry.generate_routes(schema, fn req -> {:ok, req} end)
          Map.merge(acc, routes)

        {mod, handlers} when is_atom(mod) ->
          if function_exported?(mod, :__schema__, 0) do
            schema = mod.__schema__()
            {:ok, routes} = Registry.generate_routes(schema, handlers)
            Map.merge(acc, routes)
          else
            acc
          end

        {%SimpleGrpc.Schema{} = schema, handlers} ->
          {:ok, routes} = Registry.generate_routes(schema, handlers)
          Map.merge(acc, routes)

        _ ->
          acc
      end
    end)
  end

  defp extract_schemas_from_services(services) when is_list(services) do
    Enum.flat_map(services, fn
      mod when is_atom(mod) ->
        if function_exported?(mod, :__schema__, 0), do: [mod.__schema__()], else: []

      %SimpleGrpc.Schema{} = s ->
        [s]

      {mod_or_s, _handlers} ->
        cond do
          is_atom(mod_or_s) && function_exported?(mod_or_s, :__schema__, 0) ->
            [mod_or_s.__schema__()]

          is_struct(mod_or_s, SimpleGrpc.Schema) ->
            [mod_or_s]

          true ->
            []
        end

      _ ->
        []
    end)
  end

  defp normalize_reflection_opts(true, schemas, services) do
    [schemas: schemas, services: services]
  end

  defp normalize_reflection_opts(opts, schemas, services) when is_list(opts) do
    [schemas: schemas, services: services] ++ opts
  end

  defp normalize_reflection_opts(_other, _schemas, _services), do: []

  defp handle_grpc_request(conn, route, start_time) do
    path = conn.request_path

    try do
      {req_arg, initial_payload_for_telemetry, conn} =
        if route.request_stream do
          req_stream = build_request_stream(conn, route)
          {req_stream, nil, conn}
        else
          case read_grpc_frame(conn, <<>>) do
            {:ok, payload_bytes, conn, _rest} ->
              case Protobuf.decode(route.input_message, payload_bytes, route.schema) do
                {:ok, req_map} ->
                  {req_map, req_map, conn}

                {:error, code, msg} ->
                  throw({:error, code, msg})
              end

            {:eof, conn} ->
              case Protobuf.decode(route.input_message, <<>>, route.schema) do
                {:ok, req_map} ->
                  {req_map, req_map, conn}

                {:error, code, msg} ->
                  throw({:error, code, msg})
              end

            {:error, reason} ->
              throw({:error, 3, "Invalid gRPC frame: #{inspect(reason)}"})
          end
        end

      case Dispatcher.dispatch(route, req_arg, conn) do
        {:ok, resp_data} ->
          if route.response_stream or is_enumerable_stream(resp_data, route) do
            send_streaming_grpc_response(conn, route, resp_data, start_time)
          else
            case Protobuf.encode(route.output_message, resp_data, route.schema) do
              {:ok, resp_bytes} ->
                emit_telemetry_stop(conn, path, route, initial_payload_for_telemetry, resp_data, 0, "", start_time)
                framed_response = frame_grpc_message(resp_bytes)
                send_grpc_response(conn, framed_response, 0, "")

              {:error, code, msg} ->
                throw({:error, code, msg})
            end
          end

        {:error, status_code, message} ->
          msg_str = to_string(message)
          emit_telemetry_stop(conn, path, route, initial_payload_for_telemetry, nil, status_code, msg_str, start_time)
          send_trailers_only(conn, status_code, msg_str)
      end
    rescue
      e in ArgumentError ->
        msg_str = Exception.message(e)
        emit_telemetry_exception(conn, path, route, :error, e, __STACKTRACE__, start_time)
        send_trailers_only(conn, 3, msg_str)

      e ->
        msg_str = Exception.message(e)
        emit_telemetry_exception(conn, path, route, :error, e, __STACKTRACE__, start_time)
        send_trailers_only(conn, 2, msg_str)
    catch
      kind, reason ->
        emit_telemetry_exception(conn, path, route, kind, reason, __STACKTRACE__, start_time)
        send_trailers_only(conn, 2, inspect(reason))
    end
  end

  defp is_enumerable_stream(data, route) do
    route.response_stream or is_struct(data, Stream) or is_function(data, 2) or
      (is_list(data) and route.response_stream)
  end

  defp build_request_stream(conn, route, initial_acc \\ <<>>) do
    Stream.resource(
      fn -> {conn, initial_acc, false} end,
      fn
        {conn, _acc, true} ->
          {:halt, {conn, <<>>, true}}

        {conn, acc, false} ->
          case read_grpc_frame(conn, acc) do
            {:ok, payload, conn, rest} ->
              case Protobuf.decode(route.input_message, payload, route.schema) do
                {:ok, decoded} ->
                  {[decoded], {conn, rest, false}}

                {:error, _code, reason} ->
                  raise RuntimeError, "Failed to decode streaming request: #{reason}"
              end

            {:eof, conn} ->
              {:halt, {conn, <<>>, true}}

            {:error, _reason} ->
              {:halt, {conn, <<>>, true}}
          end
      end,
      fn _ -> :ok end
    )
  end

  defp send_streaming_grpc_response(conn, route, resp_enumerable, start_time) do
    path = conn.request_path
    trailers = [
      {"grpc-status", "0"},
      {"grpc-message", ""}
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

        conn =
          Enum.reduce(resp_enumerable, conn, fn item, conn_acc ->
            case Protobuf.encode(route.output_message, item, route.schema) do
              {:ok, resp_bytes} ->
                framed = frame_grpc_message(resp_bytes)
                {:ok, conn_acc} = chunk(conn_acc, framed)
                conn_acc

              {:error, code, msg} ->
                throw({:error, code, msg})
            end
          end)

        emit_telemetry_stop(conn, path, route, nil, :stream, 0, "", start_time)

        # Send trailing headers (trailers) with END_STREAM flag on HTTP/2
        send(conn_pid, {{:send_headers, trailers, true}, stream_id})
        conn

      _other ->
        conn =
          conn
          |> put_resp_header("content-type", "application/grpc")
          |> put_resp_header("grpc-accept-encoding", "identity")
          |> send_chunked(200)

        conn =
          Enum.reduce(resp_enumerable, conn, fn item, conn_acc ->
            {:ok, resp_bytes} = Protobuf.encode(route.output_message, item, route.schema)
            framed = frame_grpc_message(resp_bytes)
            {:ok, conn_acc} = chunk(conn_acc, framed)
            conn_acc
          end)

        emit_telemetry_stop(conn, path, route, nil, :stream, 0, "", start_time)
        conn
    end
  end

  defp emit_telemetry_start(conn, path, route, start_time) do
    meta = %{
      path: path,
      headers: conn.req_headers,
      conn: conn,
      service: route && route.service_name,
      service_full_name: route && route.service_full_name,
      rpc: route && route.rpc_name
    }

    measurements = %{
      system_time: System.system_time(),
      monotonic_time: start_time
    }

    :telemetry.execute([:simple_grpc, :call, :start], measurements, meta)
  end

  defp emit_telemetry_stop(conn, path, route, req_map, resp_data, status_code, status_message, start_time) do
    stop_time = System.monotonic_time()
    duration = stop_time - start_time

    meta = %{
      path: path,
      headers: conn.req_headers,
      conn: conn,
      request: req_map,
      response: resp_data,
      grpc_status: status_code,
      grpc_message: status_message,
      service: route && route.service_name,
      service_full_name: route && route.service_full_name,
      rpc: route && route.rpc_name
    }

    measurements = %{
      duration: duration,
      monotonic_time: stop_time
    }

    :telemetry.execute([:simple_grpc, :call, :stop], measurements, meta)
  end

  defp emit_telemetry_exception(conn, path, route, kind, reason, stacktrace, start_time) do
    stop_time = System.monotonic_time()
    duration = stop_time - start_time

    meta = %{
      path: path,
      headers: conn.req_headers,
      conn: conn,
      kind: kind,
      reason: reason,
      stacktrace: stacktrace,
      service: route && route.service_name,
      service_full_name: route && route.service_full_name,
      rpc: route && route.rpc_name
    }

    measurements = %{
      duration: duration,
      monotonic_time: stop_time
    }

    :telemetry.execute([:simple_grpc, :call, :exception], measurements, meta)
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
        %{conn | state: :sent}

      _other ->
        conn
        |> put_resp_header("content-type", "application/grpc")
        |> put_resp_header("grpc-status", to_string(status_code))
        |> put_resp_header("grpc-message", to_string(status_message))
        |> send_resp(200, "")
    end
  end

  defp read_grpc_frame(conn, acc) do
    case acc do
      <<_compressed::8, length::32-big, payload::binary-size(length), rest::binary>> ->
        {:ok, payload, conn, rest}

      <<_compressed::8, length::32-big, partial::binary>> ->
        needed = length - byte_size(partial)

        case read_body(conn, length: needed) do
          {:ok, chunk, conn} ->
            acc2 = acc <> chunk

            case acc2 do
              <<_compressed::8, ^length::32-big, payload::binary-size(^length), rest::binary>> ->
                {:ok, payload, conn, rest}

              <<>> ->
                {:eof, conn}

              _ ->
                {:error, "incomplete gRPC payload frame"}
            end

          {:more, chunk, conn} ->
            read_grpc_frame(conn, acc <> chunk)

          {:error, reason} ->
            {:error, reason}
        end

      _ ->
        case read_body(conn, length: 5) do
          {:ok, chunk, conn} ->
            acc2 = acc <> chunk

            case acc2 do
              <<_compressed::8, length::32-big, payload::binary-size(length), rest::binary>> ->
                {:ok, payload, conn, rest}

              <<_compressed::8, _length::32-big, _partial::binary>> ->
                read_grpc_frame(conn, acc2)

              <<>> ->
                {:eof, conn}

              _ ->
                {:error, "incomplete gRPC header frame"}
            end

          {:more, chunk, conn} ->
            read_grpc_frame(conn, acc <> chunk)

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Unframes a 5-byte length-prefixed gRPC message.
  """
  def unframe_grpc_message(binary) do
    case SimpleGrpc.Framing.unframe(binary) do
      {:ok, data, _rest} -> {:ok, data}
      {:incomplete, <<>>} -> {:ok, <<>>}
      {:incomplete, partial} -> {:error, "Invalid gRPC frame header: #{inspect(partial)}"}
    end
  end

  @doc """
  Frames a binary payload with the standard 5-byte gRPC prefix.
  """
  defdelegate frame_grpc_message(payload), to: SimpleGrpc.Framing, as: :frame
end
