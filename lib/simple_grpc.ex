defmodule SimpleGrpc do
  @moduledoc """
  Documentation for `SimpleGrpc`.

  Simple gRPC provides lightweight, compile-time and runtime gRPC server
  and client capabilities in Elixir without requiring `protoc` or ahead-of-time
  code generation.
  """

  alias SimpleGrpc.{Registry, Schema, Protobuf}

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
end
