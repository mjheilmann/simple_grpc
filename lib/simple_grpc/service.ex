defmodule SimpleGrpc.Service do
  @moduledoc """
  What a module must expose to be served as one gRPC service.

  `use SimpleGrpc.Server` implements this behaviour; `SimpleGrpc.Plug.Service` consumes it.
  A module implements exactly one proto `service`.

    * `__schema__/0` - the `%SimpleGrpc.Schema{}` built from the module's listed files
    * `__service_name__/0` - the proto service name, e.g. `"Greeter"`
    * `__grpc_service_path__/0` - the path prefix of the service's RPCs, e.g. `"/pkg.Greeter"`
    * `__rpcs__/0` - the service's RPC definitions
    * `__interceptors__/0` - interceptors declared on the module
  """

  alias SimpleGrpc.Schema

  @callback __schema__() :: Schema.t()
  @callback __service_name__() :: String.t()
  @callback __grpc_service_path__() :: String.t()
  @callback __rpcs__() :: term()
  @callback __interceptors__() :: list()

  @callbacks [
    __schema__: 0,
    __service_name__: 0,
    __grpc_service_path__: 0,
    __rpcs__: 0,
    __interceptors__: 0
  ]

  @doc false
  # Raises `ArgumentError` unless `module` implements this behaviour.
  def validate!(module) when is_atom(module) do
    # `ensure_compiled` (not `ensure_loaded`) so that, when a pipeline is compiled in the same
    # project as the server it names, the compiler waits for the server module.
    Code.ensure_compiled(module)

    case Enum.reject(@callbacks, fn {fun, arity} -> function_exported?(module, fun, arity) end) do
      [] ->
        :ok

      missing ->
        raise ArgumentError,
              "#{inspect(module)} is not a SimpleGrpc.Service " <>
                "(missing #{Enum.map_join(missing, ", ", fn {f, a} -> "#{f}/#{a}" end)}). " <>
                "Define it with `use SimpleGrpc.Server`."
    end
  end

  def validate!(other) do
    raise ArgumentError, "expected a module that uses SimpleGrpc.Server, got: #{inspect(other)}"
  end

  @doc false
  # `routes/1`, built on first use and kept in `:persistent_term` (a cache of a pure function of the
  # compiled module, keyed by its md5 so a recompiled module is rebuilt). Plug state must stay
  # escapable because `Plug.Builder` runs `init/1` at compile time and embeds the result in the
  # pipeline module; a route table holds closures and the whole schema, so it cannot live there.
  def cached_routes(server) do
    key = {__MODULE__, server, server.module_info(:md5)}

    case :persistent_term.get(key, nil) do
      nil ->
        routes = routes(server)
        :persistent_term.put(key, routes)
        routes

      routes ->
        routes
    end
  end

  @doc false
  # The route table for `server`: `%{path => route}` for the RPCs of its one service only, with
  # each handler resolved up front. An RPC without a handler function gets a handler that
  # answers `UNIMPLEMENTED`.
  def routes(server) do
    validate!(server)

    schema = server.__schema__()
    full_name = server |> apply(:__grpc_service_path__, []) |> String.trim_leading("/")

    service =
      Enum.find_value(schema.services, fn {_key, service} ->
        if service.full_name == full_name, do: service
      end) ||
        raise ArgumentError,
              "#{inspect(server)} has no service #{inspect(full_name)} in its schema"

    interceptors = List.wrap(server.__interceptors__())

    Map.new(service.rpcs, fn {_name, rpc} ->
      route = %{
        server: server,
        service_name: service.name,
        service_full_name: service.full_name,
        package: service.package,
        rpc_name: rpc.name,
        path: rpc.path,
        input_type: rpc.input,
        output_type: rpc.output,
        input_message: Schema.resolve_message(schema, rpc.input, service.package),
        output_message: Schema.resolve_message(schema, rpc.output, service.package),
        request_stream: rpc.request_stream,
        response_stream: rpc.response_stream,
        schema: schema,
        handler: resolve_handler(server, rpc.name),
        interceptors: interceptors
      }

      {rpc.path, route}
    end)
  end

  # Mirrors the lookup order of `SimpleGrpc.Dispatcher` for a module handler, done once.
  defp resolve_handler(server, rpc_name) do
    snake = rpc_name |> Macro.underscore() |> String.to_atom()
    orig = String.to_atom(rpc_name)

    cond do
      function_exported?(server, snake, 2) -> &apply(server, snake, [&1, &2])
      function_exported?(server, snake, 1) -> &apply(server, snake, [&1])
      function_exported?(server, orig, 2) -> &apply(server, orig, [&1, &2])
      function_exported?(server, orig, 1) -> &apply(server, orig, [&1])
      true -> custom_or_unimplemented(server, rpc_name)
    end
  end

  defp custom_or_unimplemented(server, rpc_name) do
    custom =
      if function_exported?(server, :__handlers__, 0),
        do: SimpleGrpc.Server.find_handler_in(server.__handlers__(), rpc_name)

    custom ||
      fn _req, _ctx ->
        {:error, 12, "No handler found for #{rpc_name} in #{inspect(server)}"}
      end
  end
end
