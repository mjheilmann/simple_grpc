defmodule SimpleGrpc.Plug do
  @moduledoc """
  A Plug that serves gRPC and gRPC-Web requests over HTTP/1.1 and HTTP/2 using `SimpleGrpc.Registry`
  and `SimpleGrpc.Protobuf`.

  > #### Superseded {: .warning}
  >
  > Prefer `SimpleGrpc.Plug.Service` (one plug per service, composed with `Plug.Builder` and
  > ended by `SimpleGrpc.Plug.NotFound`). This plug serves a list of services from one route map,
  > which cannot keep services that share RPC names apart, and falls back to the global
  > `SimpleGrpc.Registry`. It will be removed once the health, reflection and test-helper pieces
  > have moved to the new plugs.
  """

  require Logger

  alias SimpleGrpc.{Call, Registry}

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
    state = normalize_state(state)

    route_lookup =
      if state.routes do
        case Map.get(state.routes, conn.request_path) do
          nil -> :not_found
          route -> {:ok, route}
        end
      else
        Registry.lookup_route(conn.request_path)
      end

    Call.serve(conn, route_lookup, state)
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

  @doc """
  Unframes a 5-byte length-prefixed gRPC message.
  """
  defdelegate unframe_grpc_message(binary), to: Call

  @doc """
  Frames a binary payload with the standard 5-byte gRPC prefix.
  """
  defdelegate frame_grpc_message(payload), to: SimpleGrpc.Framing, as: :frame
  defdelegate frame_grpc_message(payload, opts), to: SimpleGrpc.Framing, as: :frame
end
