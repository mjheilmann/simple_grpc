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
        handle_grpc_request(conn, route)

      :not_found ->
        if state.unmatched == :pass do
          conn
        else
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

  defp handle_grpc_request(conn, route) do
    with {:ok, payload_bytes, conn} <- read_grpc_frame(conn),
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
        %{conn | state: :sent}

      _other ->
        conn
        |> put_resp_header("content-type", "application/grpc")
        |> put_resp_header("grpc-status", to_string(status_code))
        |> put_resp_header("grpc-message", to_string(status_message))
        |> send_resp(200, "")
    end
  end

  defp read_grpc_frame(conn, acc \\ <<>>) do
    case acc do
      <<_compressed::8, length::32-big, payload::binary-size(length), _rest::binary>> ->
        {:ok, payload, conn}

      <<_compressed::8, length::32-big, partial::binary>> ->
        needed = length - byte_size(partial)

        case read_body(conn, length: needed) do
          {:ok, chunk, conn} ->
            acc2 = acc <> chunk

            case acc2 do
              <<_compressed::8, ^length::32-big, payload::binary-size(^length), _rest::binary>> ->
                {:ok, payload, conn}

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
              <<_compressed::8, length::32-big, payload::binary-size(length), _rest::binary>> ->
                {:ok, payload, conn}

              <<_compressed::8, _length::32-big, _partial::binary>> ->
                read_grpc_frame(conn, acc2)

              <<>> ->
                {:ok, <<>>, conn}

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
