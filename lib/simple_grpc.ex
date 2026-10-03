defmodule SimpleGrpc do
  @moduledoc """
  Lightweight, pure-Elixir gRPC server and client with zero external compiler dependencies (`protoc`-free).

  ## Features
  - **No `protoc` required**: Pure-Elixir `.proto` parsing, protobuf encoding/decoding, and schema synthesis.
  - **Fast Server**: Built on Plug & Bandit with full HTTP/2 streaming and server reflection support.
  - **Type-Safe Client**: Strongly typed compile-time clients (`use SimpleGrpc.Client`), lazy server streaming (`Stream.resource`), and progressive client streaming.
  - **In-Memory Testing**: Direct `:plug` adapter for zero-overhead, socket-free client-to-server integration testing.
  - **gRPC Server Reflection**: Dynamic reflection client and server (`v1` and `v1alpha`).

  ## Server Quickstart
      defmodule GreeterServer do
        use SimpleGrpc.Server,
          server_file: "priv/protos/greeter.proto"

        def say_hello(req, _stream) do
          {:ok, %{message: "Hello, " <> req.name}}
        end
      end

      # In your supervision tree or Bandit endpoint:
      {Bandit, plug: {SimpleGrpc.Plug, services: [GreeterServer], reflection: true}, port: 50051}

  ## Typed Client Quickstart
      defmodule GreeterClient do
        use SimpleGrpc.Client,
          client_file: "priv/protos/greeter.proto"
      end

      {:ok, conn} = SimpleGrpc.Client.connect("localhost:50051", adapter: :mint)
      {:ok, response} = GreeterClient.say_hello(conn, %{name: "Alice"})

  ## In-Memory Testing (Zero TCP / Zero Sockets)
      {:ok, conn} = SimpleGrpc.Client.connect(SimpleGrpc.Plug, adapter: :plug)
      {:ok, response} = GreeterClient.say_hello(conn, %{name: "Test"})

  ## Dynamic Reflection Client
      {:ok, client} = SimpleGrpc.Client.connect("localhost:50051", reflection: true)
      {:ok, response} = SimpleGrpc.Client.call(client, "greeter.Greeter/SayHello", %{name: "Dynamic"})
  """

  alias SimpleGrpc.{Registry, Schema, Protobuf}

  @doc """
  Connects to a gRPC server endpoint. Delegates to `SimpleGrpc.Client.connect/2`.
  """
  defdelegate connect(target, opts \\ []), to: SimpleGrpc.Client

  @doc """
  Invokes an RPC dynamically using reflection or a runtime schema. Delegates to `SimpleGrpc.Client.call/4`.
  """
  defdelegate call(conn, rpc_or_path, payload, opts \\ []), to: SimpleGrpc.Client

  @doc """
  Registers a schema and its handlers with the dynamic gRPC registry.
  """
  def register_service(schema_or_module, handlers \\ nil)

  def register_service(%Schema{} = schema, handlers) do
    Registry.register_service(schema, handlers)
  end

  def register_service(server_module, handlers) when is_atom(server_module) do
    schema = server_module.__schema__()
    handler_spec = handlers || server_module
    Registry.register_service(schema, handler_spec)
  end

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
end
