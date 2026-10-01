defmodule SimpleGrpc.Registry do
  @moduledoc """
  High-performance, dynamic service and RPC route registry.

  Stores route metadata and handlers in `:persistent_term` for zero-lock,
  multi-core concurrent reads during gRPC request processing, while supporting
  runtime addition, replacement, and removal of services and methods.
  """

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.{Service, RPC}

  @persistent_key {__MODULE__, :routes}

  @doc """
  Registers all RPCs defined in a `SimpleGrpc.Schema` with their corresponding handlers.

  `handlers` can be:
  - A module (e.g. `MyServer`)
  - A map/keyword list of `%{rpc_name => handler}`
  - A single function `fn (rpc_name, req, stream) -> resp end` or `fn (req, stream) -> resp end`
  """
  def register_service(%Schema{} = schema, handlers) do
    {:ok, routes} = generate_routes(schema, handlers)

    current_routes = get_all_routes()
    new_routes = Map.merge(current_routes, routes)
    put_all_routes(new_routes)
    {:ok, Map.keys(routes)}
  end

  @doc """
  Generates a map of canonical gRPC paths to route descriptors from a schema and handlers.
  """
  def generate_routes(schema, handlers) do
    Enum.reduce_while(schema.services, [], fn {_s_name, %Service{} = service}, acc ->
      case map_service(service, schema, handlers) do
        {:ok, list} -> {:cont, [list | acc]}
        {:error, reason} -> {:halt, :error, reason}
      end
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      routes -> {:ok, routes |> List.flatten() |> Map.new()}
    end
  end

  defp map_service(%Service{} = service, schema, handlers) do
    service.rpcs
    |> Enum.reduce_while([], fn {_r_name, %RPC{} = rpc}, acc ->
      case generate_route(schema, rpc, handlers, service) do
        {:ok, route} -> {:cont, [{rpc.path, route} | acc]}
        {:error, reason} -> {:halt, :error, reason}
      end
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      list -> {:ok, list}
    end
  end

  defp generate_route(schema, rpc, handlers, service) do
    with {:ok, handler} <- resolve_rpc_handler(handlers, rpc.name) do
      input_msg = Schema.message(schema, rpc.input)
      output_msg = Schema.message(schema, rpc.output)

      {:ok,
       %{
         service_name: service.name,
         service_full_name: service.full_name,
         package: service.package,
         rpc_name: rpc.name,
         path: rpc.path,
         input_type: rpc.input,
         output_type: rpc.output,
         input_message: input_msg,
         output_message: output_msg,
         request_stream: rpc.request_stream,
         response_stream: rpc.response_stream,
         schema: schema,
         handler: handler
       }}
    end
  end

  @doc """
  Registers or replaces a single RPC route dynamically at runtime.
  """
  def register_rpc(path, route_info) when is_binary(path) and is_map(route_info) do
    current_routes = get_all_routes()
    updated_route = Map.put(route_info, :path, path)
    new_routes = Map.put(current_routes, path, updated_route)
    put_all_routes(new_routes)
    {:ok, path}
  end

  @doc """
  Unregisters a route by its canonical gRPC path.
  """
  def unregister_rpc(path) when is_binary(path) do
    current_routes = get_all_routes()
    new_routes = Map.delete(current_routes, path)
    put_all_routes(new_routes)
    :ok
  end

  @doc """
  Unregisters all routes belonging to a given service name or prefix.
  """
  def unregister_service(service_name_or_prefix) when is_binary(service_name_or_prefix) do
    current_routes = get_all_routes()

    new_routes =
      Enum.reject(current_routes, fn {_path, route} ->
        route.service_name == service_name_or_prefix or
          route.service_full_name == service_name_or_prefix or
          String.starts_with?(route.path, "/#{service_name_or_prefix}/") or
          String.starts_with?(route.path, service_name_or_prefix)
      end)
      |> Map.new()

    put_all_routes(new_routes)
    :ok
  end

  @doc """
  Looks up a registered route by its gRPC HTTP path.
  """
  def lookup_route(path) when is_binary(path) do
    case Map.get(get_all_routes(), path) do
      nil -> :not_found
      route -> {:ok, route}
    end
  end

  @doc """
  Lists all currently registered routes.
  """
  def list_routes do
    Map.values(get_all_routes())
  end

  @doc """
  Lists all unique registered service names.
  """
  def list_services do
    get_all_routes()
    |> Map.values()
    |> Enum.map(& &1.service_full_name)
    |> Enum.uniq()
  end

  @doc """
  Lists all unique schemas currently registered across routes.
  """
  def list_schemas do
    get_all_routes()
    |> Map.values()
    |> Enum.map(& &1.schema)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq_by(fn s -> {s.package, Map.keys(s.services), Map.keys(s.messages)} end)
  end

  @doc """
  Clears all registered routes.
  """
  def clear do
    put_all_routes(%{})
    :ok
  end

  # ---------------------------------------------------------------------------
  # Storage Helpers (:persistent_term)
  # ---------------------------------------------------------------------------

  defp get_all_routes do
    :persistent_term.get(@persistent_key, %{})
  rescue
    _ -> %{}
  end

  defp put_all_routes(routes) when is_map(routes) do
    :persistent_term.put(@persistent_key, routes)
  end

  defp resolve_rpc_handler(handlers, rpc_name) do
    snake_atom = String.to_atom(Macro.underscore(rpc_name))
    orig_atom = String.to_atom(rpc_name)
    snake_str = Macro.underscore(rpc_name)

    handler =
      cond do
        is_atom(handlers) ->
          handlers

        Keyword.keyword?(handlers) ->
          Keyword.get(handlers, snake_atom) || Keyword.get(handlers, orig_atom)

        is_map(handlers) ->
          Map.get(handlers, snake_atom) ||
            Map.get(handlers, orig_atom) ||
            Map.get(handlers, rpc_name) ||
            Map.get(handlers, snake_str)

        is_function(handlers) ->
          handlers

        true ->
          :none
      end

    case handler do
      :none -> {:error, "Invalid handlers specification: #{inspect(handlers)}"}
      handler -> {:ok, handler}
    end
  end
end
