defmodule SimpleGrpc.Client do
  @moduledoc """
  gRPC Client for SimpleGrpc supporting pure-Elixir HTTP/2 (via Mint) and direct in-memory testing (via Plug).
  """

  alias SimpleGrpc.Client.Conn

  @doc """
  Connects to a gRPC server target using the specified or auto-detected adapter.

  ## Options
  - `:adapter` - Transport adapter module (`SimpleGrpc.Client.Adapters.Mint` or `SimpleGrpc.Client.Adapters.Plug`, or shorthand `:mint` / `:plug`).
  - `:scheme` - `:http` (default) or `:https`.
  - `:timeout` - Request timeout in milliseconds (default: `5000`).
  - `:headers` - Additional default request headers / metadata.
  """
  def connect(target, opts \\ []) do
    adapter = resolve_adapter(target, opts)

    case adapter.connect(target, opts) do
      {:ok, adapter_state} ->
        {:ok,
         %Conn{
           adapter: adapter,
           state: adapter_state,
           target: target,
           opts: opts
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Closes the client connection.
  """
  def close(%Conn{adapter: adapter, state: state}) do
    adapter.close(state)
  end

  @doc """
  Sends a unary RPC request to the given canonical gRPC path with an encoded binary payload.
  """
  def send_unary(%Conn{adapter: adapter, state: state}, path, payload, opts \\ []) do
    adapter.send_unary(state, path, payload, opts)
  end

  @doc """
  Sends a streaming RPC request to the given canonical gRPC path.
  """
  def send_stream(%Conn{adapter: adapter, state: state}, path, request_data, opts \\ []) do
    adapter.send_stream(state, path, request_data, opts)
  end

  defp resolve_adapter(target, opts) do
    case Keyword.get(opts, :adapter) do
      :mint ->
        SimpleGrpc.Client.Adapters.Mint

      :plug ->
        SimpleGrpc.Client.Adapters.Plug

      mod when is_atom(mod) and not is_nil(mod) ->
        mod

      nil ->
        if (is_atom(target) and function_exported?(target, :init, 1)) or
             Keyword.has_key?(opts, :plug) do
          SimpleGrpc.Client.Adapters.Plug
        else
          SimpleGrpc.Client.Adapters.Mint
        end
    end
  end
end
