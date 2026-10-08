defmodule SimpleGrpc do
  @moduledoc """
  Lightweight, pure-Elixir gRPC server and client with zero external compiler dependencies (`protoc`-free).

  ## Features
  - **No `protoc` required**: Pure-Elixir `.proto` parsing, protobuf encoding/decoding, and schema synthesis.
  - **Fast Server**: Built on Plug & Bandit with full HTTP/2 streaming and server reflection support.
  - **Type-Safe Client**: Strongly typed compile-time clients (`use SimpleGrpc.Client`), lazy server streaming (`Stream.resource`), and progressive client streaming.
  - **In-Memory Testing**: Direct `:plug` adapter for zero-overhead, socket-free client-to-server integration testing.
  - **gRPC Server Reflection**: Dynamic reflection client and server (`v1` and `v1alpha`).

  ## Server Documentation (`SimpleGrpc.Server`)
  To implement a gRPC server, use `SimpleGrpc.Server`:

      defmodule GreeterServer do
        use SimpleGrpc.Server,
          server_file: "priv/protos/greeter.proto"

        def say_hello(req, _stream) do
          {:ok, %{message: "Hello, " <> req.name}}
        end
      end

      # In your supervision tree or Bandit endpoint:
      {Bandit, plug: {SimpleGrpc.Plug, services: [GreeterServer], reflection: true}, port: 50051}

  See `SimpleGrpc.Server` for detailed options, streaming handlers, and lifecycle hooks.

  ## Client Documentation (`SimpleGrpc.Client`)
  To interact with a gRPC server, use `SimpleGrpc.Client`:

  ### 1. Strongly Typed Client
      defmodule GreeterClient do
        use SimpleGrpc.Client,
          client_file: "priv/protos/greeter.proto"
      end

      {:ok, conn} = SimpleGrpc.Client.connect("localhost:50051", adapter: :mint)
      {:ok, response} = GreeterClient.say_hello(conn, %{name: "Alice"})

  ### 2. In-Memory Testing (Zero TCP / Zero Sockets)
      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)
      {:ok, response} = GreeterClient.say_hello(conn, %{name: "Test"})

  ### 3. Dynamic Reflection Client
      {:ok, client} = SimpleGrpc.Client.connect("localhost:50051", reflection: true)
      {:ok, response} = SimpleGrpc.Client.call(client, "greeter.Greeter/SayHello", %{name: "Dynamic"})

  See `SimpleGrpc.Client` for streaming calls, transport adapters, headers, and timeout options.
  """

  alias SimpleGrpc.{Registry, Protobuf}

  @doc """
  Connects to a gRPC server endpoint. Delegates to `SimpleGrpc.Client.connect/2`.
  """
  defdelegate connect(target, opts \\ []), to: SimpleGrpc.Client

  @doc """
  Invokes an RPC dynamically using reflection or a runtime schema. Delegates to `SimpleGrpc.Client.call/4`.
  """
  defdelegate call(conn, rpc_or_path, payload, opts \\ []), to: SimpleGrpc.Client

  @doc """
  Registers a schema or server module and its handlers with the dynamic gRPC registry. Delegates to `SimpleGrpc.Registry.register_service/2`.
  """
  defdelegate register_service(schema_or_module, handlers \\ nil), to: Registry

  @doc """
  Registers or replaces an individual RPC route dynamically at runtime.
  """
  defdelegate register_rpc(path, route_info), to: Registry

  @doc """
  Unregisters an individual RPC route.
  """
  defdelegate unregister_rpc(path), to: Registry

  @doc """
  Unregisters all routes belonging to a given service name or prefix.
  """
  defdelegate unregister_service(service_name_or_prefix), to: Registry

  @doc """
  Looks up a route by gRPC HTTP path.
  """
  defdelegate lookup_route(path), to: Registry

  @doc """
  Lists all currently registered service names.
  """
  defdelegate list_services(), to: Registry

  @doc """
  Encodes a message into protobuf binary format.
  """
  defdelegate encode(message_or_mod, data, schema_ctx \\ nil), to: Protobuf

  @doc """
  Decodes a protobuf binary into an Elixir map.
  """
  defdelegate decode(message_or_mod, binary, schema_ctx \\ nil), to: Protobuf

  @doc """
  Enables gRPC Server Reflection (`grpc.reflection.v1` and `grpc.reflection.v1alpha`).
  Options:
  - `:only` - list of service names or module atoms to reflect
  - `:except` - list of service names or module atoms to exclude
  - `:services` - list of specific services or modules to reflect
  - `:schemas` - list of `%SimpleGrpc.Schema{}` structs to reflect
  """
  defdelegate enable_reflection(opts \\ []), to: SimpleGrpc.Reflection.Server, as: :register

  @doc """
  Enables the canonical gRPC Health Checking service (`grpc.health.v1.Health`).
  """
  defdelegate enable_health(opts \\ []), to: SimpleGrpc.Health, as: :register

  @doc """
  Returns the deadline `DateTime` for the active incoming request, or `nil` if no `grpc-timeout` header was provided.
  """
  def deadline(%Plug.Conn{} = conn) do
    case Plug.Conn.get_req_header(conn, "grpc-timeout") do
      [timeout_header | _] ->
        case SimpleGrpc.Client.Headers.parse_grpc_timeout(timeout_header) do
          {:ok, ms} ->
            DateTime.add(DateTime.utc_now(), ms, :millisecond)

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  def deadline(_), do: nil

  @doc """
  Returns the remaining milliseconds before the deadline for the active request expires.
  Returns `nil` if no deadline was set, or `0` if the deadline has already passed.
  """
  def remaining_timeout(conn, unit \\ :millisecond)

  def remaining_timeout(%Plug.Conn{} = conn, unit) do
    case Plug.Conn.get_req_header(conn, "grpc-timeout") do
      [timeout_header | _] ->
        case SimpleGrpc.Client.Headers.parse_grpc_timeout(timeout_header) do
          {:ok, ms} ->
            case unit do
              :millisecond -> ms
              :second -> div(ms, 1000)
              :microsecond -> ms * 1000
            end

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  def remaining_timeout(_, _), do: nil
end
