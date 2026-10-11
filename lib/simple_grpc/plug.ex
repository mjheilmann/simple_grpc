defmodule SimpleGrpc.Plug do
  @moduledoc """
  A Plug that serves gRPC and gRPC-Web requests over HTTP/1.1 and HTTP/2 using `SimpleGrpc.Registry`
  and `SimpleGrpc.Protobuf`.
  """

  import Plug.Conn
  require Logger

  alias SimpleGrpc.{Registry, Dispatcher, Protobuf, Status, GrpcWeb, Framing}

  @behaviour Plug

  @doc """
  Initializes the Plug with gRPC services, interceptors, reflection, health check, and CORS options.
  """
  @impl true
  def init(opts) do
    opts_map = Enum.into(opts, %{})

    services =
      cond do
        Map.has_key?(opts_map, :services) -> Map.get(opts_map, :services)
        Map.has_key?(opts_map, :server) -> [Map.get(opts_map, :server)]
        true -> nil
      end

    reflection_opt = Map.get(opts_map, :reflection, false)
    health_opt = Map.get(opts_map, :health, false)
    unmatched = Map.get(opts_map, :unmatched, :unimplemented)
    interceptors = Map.get(opts_map, :interceptors, [])

    cors_opt = Map.get(opts_map, :cors, true)

    cors_opts =
      case cors_opt do
        false -> nil
        true -> [allowed_origins: Map.get(opts_map, :allowed_origins, "*")]
        opts when is_list(opts) -> opts
        _ -> [allowed_origins: "*"]
      end

    routes =
      if services || health_opt || (reflection_opt && services) do
        base_routes = if services, do: build_service_routes(services), else: %{}

        routes_with_refl =
          if reflection_opt do
            schemas = if services, do: extract_schemas_from_services(services), else: nil
            refl_opts = normalize_reflection_opts(reflection_opt, schemas, services)
            refl_routes = SimpleGrpc.Reflection.Server.build_reflection_routes(refl_opts)
            Map.merge(base_routes, refl_routes)
          else
            base_routes
          end

        if health_opt do
          health_opts = if is_list(health_opt), do: health_opt, else: []
          health_routes = SimpleGrpc.Health.build_health_routes(health_opts)
          Map.merge(routes_with_refl, health_routes)
        else
          routes_with_refl
        end
      else
        nil
      end

    %{
      routes: routes,
      unmatched: unmatched,
      interceptors: List.wrap(interceptors),
      cors_opts: cors_opts
    }
  end

  @doc """
  Processes an incoming gRPC or gRPC-Web request.
  """
  @impl true
  def call(conn, state) do
    start_time = System.monotonic_time()
    path = conn.request_path
    state = normalize_state(state)

    if state.cors_opts && GrpcWeb.cors_preflight?(conn) do
      GrpcWeb.handle_cors_preflight(conn, state.cors_opts)
    else
      grpc_web_format = GrpcWeb.format(conn)

      # Check for unsupported grpc-encoding header
      encoding = get_req_header(conn, "grpc-encoding") |> List.first()

      if encoding not in [nil, ""] and not SimpleGrpc.Compression.supported_encoding?(encoding) do
        emit_telemetry_start(conn, path, nil, start_time)

        emit_telemetry_stop(
          conn,
          path,
          nil,
          nil,
          nil,
          12,
          "unsupported grpc-encoding: #{encoding}",
          start_time
        )

        send_trailers_only(
          conn,
          12,
          "unsupported grpc-encoding: #{encoding}",
          nil,
          grpc_web_format,
          state.cors_opts
        )
      else
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

            handle_grpc_request(
              conn,
              route,
              start_time,
              state.interceptors,
              grpc_web_format,
              state.cors_opts
            )

          :not_found ->
            if state.unmatched == :pass do
              conn
            else
              emit_telemetry_start(conn, path, nil, start_time)

              emit_telemetry_stop(
                conn,
                path,
                nil,
                nil,
                nil,
                12,
                "unknown service or method: #{path}",
                start_time
              )

              send_trailers_only(
                conn,
                12,
                "unknown service or method: #{path}",
                nil,
                grpc_web_format,
                state.cors_opts
              )
            end
        end
      end
    end
  end

  defp normalize_state(%{unmatched: _, interceptors: _, cors_opts: _} = state), do: state

  defp normalize_state(%{unmatched: _, interceptors: _} = state),
    do: Map.put(state, :cors_opts, allowed_origins: "*")

  defp normalize_state(%{unmatched: _} = state),
    do: Map.merge(state, %{interceptors: [], cors_opts: [allowed_origins: "*"]})

  defp normalize_state(opts) when is_list(opts), do: init(opts)

  defp normalize_state(_),
    do: %{
      routes: nil,
      unmatched: :unimplemented,
      interceptors: [],
      cors_opts: [allowed_origins: "*"]
    }

  defp build_service_routes(services) when is_list(services) do
    Enum.reduce(services, %{}, fn item, acc ->
      case item do
        mod when is_atom(mod) ->
          Code.ensure_loaded(mod)

          if function_exported?(mod, :__schema__, 0) do
            schema = mod.__schema__()
            {:ok, routes} = Registry.generate_routes(schema, mod)

            interceptors =
              if function_exported?(mod, :__interceptors__, 0),
                do: mod.__interceptors__(),
                else: []

            routes_with_interceptors = attach_interceptors_to_routes(routes, interceptors)
            Map.merge(acc, routes_with_interceptors)
          else
            acc
          end

        %SimpleGrpc.Schema{} = schema ->
          {:ok, routes} = Registry.generate_routes(schema, fn req -> {:ok, req} end)
          Map.merge(acc, routes)

        {mod, handlers} when is_atom(mod) ->
          Code.ensure_loaded(mod)

          if function_exported?(mod, :__schema__, 0) do
            schema = mod.__schema__()
            {:ok, routes} = Registry.generate_routes(schema, handlers)

            interceptors =
              if function_exported?(mod, :__interceptors__, 0),
                do: mod.__interceptors__(),
                else: []

            routes_with_interceptors = attach_interceptors_to_routes(routes, interceptors)
            Map.merge(acc, routes_with_interceptors)
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

  defp attach_interceptors_to_routes(routes, interceptors) when is_map(routes) do
    Map.new(routes, fn {path, route} ->
      {path, Map.put(route, :interceptors, List.wrap(interceptors))}
    end)
  end

  defp extract_schemas_from_services(services) when is_list(services) do
    Enum.flat_map(services, fn
      mod when is_atom(mod) ->
        Code.ensure_loaded(mod)
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
    opts = []
    opts = if schemas, do: [{:schemas, schemas} | opts], else: opts
    opts = if services, do: [{:services, services} | opts], else: opts
    opts
  end

  defp normalize_reflection_opts(opts, schemas, services) when is_list(opts) do
    base = []
    base = if schemas, do: [{:schemas, schemas} | base], else: base
    base = if services, do: [{:services, services} | base], else: base
    base ++ opts
  end

  defp normalize_reflection_opts(_other, _schemas, _services), do: []

  defp handle_grpc_request(conn, route, start_time, plug_interceptors, grpc_web_format, cors_opts) do
    path = conn.request_path
    timeout_header = get_req_header(conn, "grpc-timeout") |> List.first()

    deadline_ms =
      case SimpleGrpc.Client.Headers.parse_grpc_timeout(timeout_header) do
        {:ok, ms} -> ms
        _ -> nil
      end

    if is_integer(deadline_ms) and deadline_ms <= 0 do
      emit_telemetry_stop(conn, path, route, nil, nil, 4, "deadline exceeded", start_time)
      send_trailers_only(conn, 4, "deadline exceeded", nil, grpc_web_format, cors_opts)
    else
      try do
        {req_arg, initial_payload_for_telemetry, conn} =
          if route.request_stream do
            req_stream = build_request_stream(conn, route, <<>>, grpc_web_format)
            {req_stream, nil, conn}
          else
            case read_grpc_frame(conn, <<>>, grpc_web_format) do
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

        route_interceptors = Map.get(route, :interceptors, [])
        all_interceptors = List.wrap(plug_interceptors) ++ List.wrap(route_interceptors)

        ctx = %SimpleGrpc.Context{
          role: :server,
          service: route.service_name,
          service_name: route.service_full_name,
          rpc: route.rpc_name,
          rpc_name: route.rpc_name,
          path: route.path,
          request_stream: route.request_stream,
          response_stream: route.response_stream,
          headers: conn.req_headers,
          req_headers: conn.req_headers,
          metadata: %{},
          private: %{},
          opts: [],
          conn: conn,
          schema: route.schema
        }

        dispatch_with_pipeline = fn ->
          SimpleGrpc.Pipeline.execute(all_interceptors, req_arg, ctx, fn req, current_ctx ->
            Dispatcher.dispatch(route, req, current_ctx)
          end)
        end

        dispatch_result =
          if is_integer(deadline_ms) and deadline_ms > 0 and not route.request_stream do
            task = Task.async(dispatch_with_pipeline)

            case Task.yield(task, deadline_ms) || Task.shutdown(task, :brutal_kill) do
              {:ok, res} -> res
              nil -> throw({:error, 4, "deadline exceeded"})
            end
          else
            dispatch_with_pipeline.()
          end

        case dispatch_result do
          {:ok, resp_data} ->
            if route.response_stream or is_enumerable_stream(resp_data, route) do
              send_streaming_grpc_response(
                conn,
                route,
                resp_data,
                start_time,
                deadline_ms,
                grpc_web_format,
                cors_opts
              )
            else
              case Protobuf.encode(route.output_message, resp_data, route.schema) do
                {:ok, resp_bytes} ->
                  emit_telemetry_stop(
                    conn,
                    path,
                    route,
                    initial_payload_for_telemetry,
                    resp_data,
                    0,
                    "",
                    start_time
                  )

                  response_encoding = negotiate_response_encoding(conn)
                  framed_response = frame_grpc_message(resp_bytes, encoding: response_encoding)

                  send_grpc_response(
                    conn,
                    framed_response,
                    0,
                    "",
                    response_encoding,
                    grpc_web_format,
                    cors_opts
                  )

                {:error, code, msg} ->
                  throw({:error, code, msg})
              end
            end

          {:error, %SimpleGrpc.RPC.Status{} = status} ->
            emit_telemetry_stop(
              conn,
              path,
              route,
              initial_payload_for_telemetry,
              nil,
              status.code,
              status.message,
              start_time
            )

            send_trailers_only(conn, status, "", nil, grpc_web_format, cors_opts)

          {:error, status_code, message, details} ->
            msg_str = to_string(message)

            emit_telemetry_stop(
              conn,
              path,
              route,
              initial_payload_for_telemetry,
              nil,
              status_code,
              msg_str,
              start_time
            )

            send_trailers_only(conn, status_code, msg_str, details, grpc_web_format, cors_opts)

          {:error, status_code, message} ->
            msg_str = to_string(message)

            emit_telemetry_stop(
              conn,
              path,
              route,
              initial_payload_for_telemetry,
              nil,
              status_code,
              msg_str,
              start_time
            )

            send_trailers_only(conn, status_code, msg_str, nil, grpc_web_format, cors_opts)
        end
      rescue
        e in ArgumentError ->
          msg_str = Exception.message(e)
          emit_telemetry_exception(conn, path, route, :error, e, __STACKTRACE__, start_time)
          send_trailers_only(conn, 3, msg_str, nil, grpc_web_format, cors_opts)

        e ->
          msg_str = Exception.message(e)
          formatted = Exception.format(:error, e, __STACKTRACE__)
          Logger.error("[SimpleGrpc.Plug] Crash in gRPC handler for #{path}:\n#{formatted}")
          emit_telemetry_exception(conn, path, route, :error, e, __STACKTRACE__, start_time)

          send_trailers_only(
            conn,
            13,
            "Internal server error: #{msg_str}",
            nil,
            grpc_web_format,
            cors_opts
          )
      catch
        :throw, {:error, %SimpleGrpc.RPC.Status{} = status} ->
          emit_telemetry_stop(
            conn,
            path,
            route,
            nil,
            nil,
            status.code,
            status.message,
            start_time
          )

          send_trailers_only(conn, status, "", nil, grpc_web_format, cors_opts)

        :throw, {:error, code, msg, details} ->
          emit_telemetry_stop(conn, path, route, nil, nil, code, to_string(msg), start_time)
          send_trailers_only(conn, code, to_string(msg), details, grpc_web_format, cors_opts)

        :throw, {:error, code, msg} ->
          emit_telemetry_stop(conn, path, route, nil, nil, code, to_string(msg), start_time)
          send_trailers_only(conn, code, to_string(msg), nil, grpc_web_format, cors_opts)

        kind, reason ->
          formatted = Exception.format(kind, reason, __STACKTRACE__)

          Logger.error(
            "[SimpleGrpc.Plug] Uncaught #{kind} in gRPC handler for #{path}:\n#{formatted}"
          )

          emit_telemetry_exception(conn, path, route, kind, reason, __STACKTRACE__, start_time)

          send_trailers_only(
            conn,
            13,
            "Internal server error: #{inspect(reason)}",
            nil,
            grpc_web_format,
            cors_opts
          )
      end
    end
  end

  defp negotiate_response_encoding(conn) do
    accept_enc = get_req_header(conn, "grpc-accept-encoding") |> Enum.join(",")
    SimpleGrpc.Compression.negotiate(accept_enc)
  end

  defp is_enumerable_stream(data, route) do
    route.response_stream or is_struct(data, Stream) or is_function(data, 2) or
      (is_list(data) and route.response_stream)
  end

  defp build_request_stream(conn, route, initial_acc, grpc_web_format) do
    Stream.resource(
      fn -> {conn, initial_acc, false} end,
      fn
        {conn, _acc, true} ->
          {:halt, {conn, <<>>, true}}

        {conn, acc, false} ->
          case read_grpc_frame(conn, acc, grpc_web_format) do
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

  defp send_streaming_grpc_response(
         conn,
         route,
         resp_enumerable,
         start_time,
         deadline_ms,
         grpc_web_format,
         cors_opts
       ) do
    path = conn.request_path
    req_ct = get_req_header(conn, "content-type") |> List.first() || ""

    trailers = [
      {"grpc-status", "0"},
      {"grpc-message", ""}
    ]

    response_encoding = negotiate_response_encoding(conn)
    accept_enc_header = SimpleGrpc.Compression.accept_encoding_header()

    deadline_end_time =
      if is_integer(deadline_ms) and deadline_ms > 0 do
        start_time + System.convert_time_unit(deadline_ms, :millisecond, :native)
      else
        nil
      end

    if grpc_web_format do
      response_ct = GrpcWeb.response_content_type(req_ct)
      trailer_frame = GrpcWeb.frame_trailers(0, "")

      case conn.adapter do
        {Bandit.Adapter, _} ->
          conn =
            conn
            |> put_resp_header("content-type", response_ct)
            |> put_resp_header("grpc-accept-encoding", accept_enc_header)
            |> maybe_put_encoding_header(response_encoding)
            |> maybe_put_cors_headers(cors_opts)
            |> send_chunked(200)

          conn =
            Enum.reduce(resp_enumerable, conn, fn item, conn_acc ->
              if deadline_end_time && System.monotonic_time() > deadline_end_time do
                throw({:error, 4, "deadline exceeded during streaming"})
              end

              case Protobuf.encode(route.output_message, item, route.schema) do
                {:ok, resp_bytes} ->
                  framed = frame_grpc_message(resp_bytes, encoding: response_encoding)
                  chunk_data = GrpcWeb.encode_response_body(framed, grpc_web_format)
                  {:ok, conn_acc} = chunk(conn_acc, chunk_data)
                  conn_acc

                {:error, code, msg} ->
                  throw({:error, code, msg})
              end
            end)

          # Send trailers frame as the final body chunk in gRPC-Web
          trailer_chunk = GrpcWeb.encode_response_body(trailer_frame, grpc_web_format)
          {:ok, conn} = chunk(conn, trailer_chunk)

          emit_telemetry_stop(conn, path, route, nil, :stream, 0, "", start_time)
          conn

        _other ->
          body =
            Enum.reduce(resp_enumerable, <<>>, fn item, acc ->
              if deadline_end_time && System.monotonic_time() > deadline_end_time do
                throw({:error, 4, "deadline exceeded during streaming"})
              end

              case Protobuf.encode(route.output_message, item, route.schema) do
                {:ok, resp_bytes} ->
                  framed = frame_grpc_message(resp_bytes, encoding: response_encoding)
                  acc <> framed

                {:error, code, msg} ->
                  throw({:error, code, msg})
              end
            end)

          full_body = body <> trailer_frame
          encoded_body = GrpcWeb.encode_response_body(full_body, grpc_web_format)

          emit_telemetry_stop(conn, path, route, nil, :stream, 0, "", start_time)

          conn
          |> put_resp_header("content-type", response_ct)
          |> put_resp_header("grpc-accept-encoding", accept_enc_header)
          |> maybe_put_encoding_header(response_encoding)
          |> maybe_put_cors_headers(cors_opts)
          |> send_resp(200, encoded_body)
      end
    else
      case conn.adapter do
        {Bandit.Adapter,
         %Bandit.Adapter{
           transport: %Bandit.HTTP2.Stream{connection_pid: conn_pid, stream_id: stream_id}
         }} ->
          conn =
            conn
            |> put_resp_header("content-type", "application/grpc")
            |> put_resp_header("grpc-accept-encoding", accept_enc_header)
            |> maybe_put_encoding_header(response_encoding)
            |> send_chunked(200)

          conn =
            Enum.reduce(resp_enumerable, conn, fn item, conn_acc ->
              if deadline_end_time && System.monotonic_time() > deadline_end_time do
                throw({:error, 4, "deadline exceeded during streaming"})
              end

              case Protobuf.encode(route.output_message, item, route.schema) do
                {:ok, resp_bytes} ->
                  framed = frame_grpc_message(resp_bytes, encoding: response_encoding)
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
          body =
            Enum.reduce(resp_enumerable, <<>>, fn item, acc ->
              if deadline_end_time && System.monotonic_time() > deadline_end_time do
                throw({:error, 4, "deadline exceeded during streaming"})
              end

              case Protobuf.encode(route.output_message, item, route.schema) do
                {:ok, resp_bytes} ->
                  framed = frame_grpc_message(resp_bytes, encoding: response_encoding)
                  acc <> framed

                {:error, code, msg} ->
                  throw({:error, code, msg})
              end
            end)

          emit_telemetry_stop(conn, path, route, nil, :stream, 0, "", start_time)

          conn
          |> put_resp_header("content-type", "application/grpc")
          |> put_resp_header("grpc-accept-encoding", accept_enc_header)
          |> maybe_put_encoding_header(response_encoding)
          |> put_resp_header("grpc-status", "0")
          |> put_resp_header("grpc-message", "")
          |> send_resp(200, body)
      end
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

  defp emit_telemetry_stop(
         conn,
         path,
         route,
         req_map,
         resp_data,
         status_code,
         status_message,
         start_time
       ) do
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

  defp send_grpc_response(
         conn,
         framed_response,
         status_code,
         status_message,
         response_encoding,
         grpc_web_format,
         cors_opts
       ) do
    req_ct = get_req_header(conn, "content-type") |> List.first() || ""

    if grpc_web_format do
      response_ct = GrpcWeb.response_content_type(req_ct)
      trailer_frame = GrpcWeb.frame_trailers(status_code, status_message)
      full_payload = framed_response <> trailer_frame
      encoded_body = GrpcWeb.encode_response_body(full_payload, grpc_web_format)

      conn
      |> put_resp_header("content-type", response_ct)
      |> put_resp_header("grpc-accept-encoding", SimpleGrpc.Compression.accept_encoding_header())
      |> maybe_put_encoding_header(response_encoding)
      |> maybe_put_cors_headers(cors_opts)
      |> send_resp(200, encoded_body)
    else
      trailers = [
        {"grpc-status", to_string(status_code)},
        {"grpc-message", to_string(status_message)}
      ]

      accept_enc_header = SimpleGrpc.Compression.accept_encoding_header()

      case conn.adapter do
        {Bandit.Adapter,
         %Bandit.Adapter{
           transport: %Bandit.HTTP2.Stream{connection_pid: conn_pid, stream_id: stream_id}
         }} ->
          conn =
            conn
            |> put_resp_header("content-type", "application/grpc")
            |> put_resp_header("grpc-accept-encoding", accept_enc_header)
            |> maybe_put_encoding_header(response_encoding)
            |> send_chunked(200)

          {:ok, conn} = chunk(conn, framed_response)

          # Send trailing headers (trailers) with END_STREAM flag on HTTP/2
          send(conn_pid, {{:send_headers, trailers, true}, stream_id})
          conn

        _other ->
          conn
          |> put_resp_header("content-type", "application/grpc")
          |> put_resp_header("grpc-accept-encoding", accept_enc_header)
          |> maybe_put_encoding_header(response_encoding)
          |> put_resp_header("grpc-status", to_string(status_code))
          |> put_resp_header("grpc-message", to_string(status_message))
          |> send_resp(200, framed_response)
      end
    end
  end

  defp maybe_put_encoding_header(conn, enc) when enc in [nil, "", "identity", :identity], do: conn

  defp maybe_put_encoding_header(conn, enc) when is_binary(enc),
    do: put_resp_header(conn, "grpc-encoding", enc)

  defp maybe_put_encoding_header(conn, enc) when is_atom(enc),
    do: put_resp_header(conn, "grpc-encoding", to_string(enc))

  defp maybe_put_encoding_header(conn, _), do: conn

  defp maybe_put_cors_headers(conn, nil), do: conn
  defp maybe_put_cors_headers(conn, false), do: conn

  defp maybe_put_cors_headers(conn, cors_opts) when is_list(cors_opts),
    do: GrpcWeb.put_cors_headers(conn, cors_opts)

  defp maybe_put_cors_headers(conn, _), do: GrpcWeb.put_cors_headers(conn)

  defp send_trailers_only(
         conn,
         status_or_code,
         status_message,
         details,
         grpc_web_format,
         cors_opts
       )

  defp send_trailers_only(
         conn,
         %SimpleGrpc.RPC.Status{} = status,
         _msg,
         _details,
         grpc_web_format,
         cors_opts
       ) do
    send_trailers_only(
      conn,
      status.code,
      status.message,
      status.details,
      grpc_web_format,
      cors_opts
    )
  end

  defp send_trailers_only(conn, status_code, status_message, details, grpc_web_format, cors_opts) do
    code_int = Status.code(status_code)
    req_ct = get_req_header(conn, "content-type") |> List.first() || ""
    grpc_web_format = grpc_web_format || GrpcWeb.format(conn)

    if grpc_web_format do
      response_ct = GrpcWeb.response_content_type(req_ct)
      trailer_frame = GrpcWeb.frame_trailers(code_int, status_message, details)
      encoded_body = GrpcWeb.encode_response_body(trailer_frame, grpc_web_format)

      conn
      |> put_resp_header("content-type", response_ct)
      |> put_resp_header("grpc-accept-encoding", SimpleGrpc.Compression.accept_encoding_header())
      |> put_resp_header("grpc-status", to_string(code_int))
      |> put_resp_header("grpc-message", URI.encode(to_string(status_message)))
      |> maybe_put_cors_headers(cors_opts)
      |> send_resp(200, encoded_body)
    else
      accept_enc_header = SimpleGrpc.Compression.accept_encoding_header()

      details_bin =
        cond do
          is_list(details) and details != [] ->
            Status.to_status_details_bin({code_int, status_message, details})

          is_binary(details) and details != "" ->
            details

          true ->
            nil
        end

      base_headers = [
        {":status", "200"},
        {"content-type", "application/grpc"},
        {"grpc-accept-encoding", accept_enc_header},
        {"grpc-status", to_string(code_int)},
        {"grpc-message", to_string(status_message)}
      ]

      headers =
        if details_bin do
          base_headers ++ [{"grpc-status-details-bin", details_bin}]
        else
          base_headers
        end

      case conn.adapter do
        {Bandit.Adapter,
         %Bandit.Adapter{
           transport: %Bandit.HTTP2.Stream{connection_pid: conn_pid, stream_id: stream_id}
         }} ->
          send(conn_pid, {{:send_headers, headers, true}, stream_id})
          %{conn | state: :sent}

        _other ->
          conn_with_headers =
            Enum.reduce(headers, conn, fn {k, v}, acc ->
              if String.starts_with?(k, ":") do
                acc
              else
                put_resp_header(acc, k, v)
              end
            end)

          send_resp(conn_with_headers, 200, "")
      end
    end
  end

  defp read_grpc_frame(conn, acc, grpc_web_format) do
    if grpc_web_format == :text do
      case read_full_body(conn, acc) do
        {:ok, body_text, conn} ->
          case GrpcWeb.decode_request_body(body_text, :text) do
            {:ok, raw_binary} ->
              req_encoding = get_req_header(conn, "grpc-encoding") |> List.first() || "gzip"

              case Framing.unframe(raw_binary, encoding: req_encoding) do
                {:ok, payload, rest} ->
                  {:ok, payload, conn, rest}

                {:incomplete, <<>>} ->
                  {:eof, conn}

                {:incomplete, partial} ->
                  {:ok, partial, conn, <<>>}

                {:error, reason} ->
                  {:error, reason}
              end

            {:error, reason} ->
              {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    else
      req_encoding = get_req_header(conn, "grpc-encoding") |> List.first() || "gzip"

      case acc do
        <<compressed::8, length::32-big, payload::binary-size(length), rest::binary>> ->
          decompressed_payload = decompress_frame(compressed, payload, req_encoding)
          {:ok, decompressed_payload, conn, rest}

        <<_compressed::8, length::32-big, partial::binary>> ->
          needed = length - byte_size(partial)

          case read_body(conn, length: needed) do
            {:ok, chunk, conn} ->
              acc2 = acc <> chunk

              case acc2 do
                <<compressed::8, ^length::32-big, payload::binary-size(^length), rest::binary>> ->
                  decompressed_payload = decompress_frame(compressed, payload, req_encoding)
                  {:ok, decompressed_payload, conn, rest}

                <<>> ->
                  {:eof, conn}

                _ ->
                  {:error, "incomplete gRPC payload frame"}
              end

            {:more, chunk, conn} ->
              read_grpc_frame(conn, acc <> chunk, grpc_web_format)

            {:error, reason} ->
              {:error, reason}
          end

        _ ->
          case read_body(conn, length: 5) do
            {:ok, chunk, conn} ->
              acc2 = acc <> chunk

              case acc2 do
                <<compressed::8, length::32-big, payload::binary-size(length), rest::binary>> ->
                  decompressed_payload = decompress_frame(compressed, payload, req_encoding)
                  {:ok, decompressed_payload, conn, rest}

                <<_compressed::8, _length::32-big, _partial::binary>> ->
                  read_grpc_frame(conn, acc2, grpc_web_format)

                <<>> ->
                  {:eof, conn}

                _ ->
                  {:error, "incomplete gRPC header frame"}
              end

            {:more, chunk, conn} ->
              read_grpc_frame(conn, acc <> chunk, grpc_web_format)

            {:error, reason} ->
              {:error, reason}
          end
      end
    end
  end

  defp read_full_body(conn, acc) do
    case read_body(conn, length: 65_536) do
      {:ok, chunk, conn} -> {:ok, acc <> chunk, conn}
      {:more, chunk, conn} -> read_full_body(conn, acc <> chunk)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decompress_frame(0, payload, _encoding), do: payload

  defp decompress_frame(1, payload, encoding) do
    case SimpleGrpc.Compression.decompress(payload, encoding) do
      {:ok, decompressed} ->
        decompressed

      {:error, reason} ->
        throw({:error, 3, "Failed to decompress gRPC payload: #{reason}"})
    end
  end

  defp decompress_frame(other, _payload, _encoding) do
    throw({:error, 12, "Unsupported compression flag: #{other}"})
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
  defdelegate frame_grpc_message(payload, opts), to: SimpleGrpc.Framing, as: :frame
end
