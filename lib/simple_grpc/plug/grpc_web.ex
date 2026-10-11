defmodule SimpleGrpc.Plug.GrpcWeb do
  @moduledoc """
  A Plug supporting gRPC-Web (`application/grpc-web` binary and `application/grpc-web-text` Base64)
  over HTTP/1.1 and HTTP/2.

  Provides automatic CORS preflight handling (`OPTIONS`), response headers exposure,
  payload Base64 decoding/encoding, and trailers-in-body framing (`0x80`).

  ## Usage as a Standalone Plug / Endpoint

  ```elixir
  plug SimpleGrpc.Plug.GrpcWeb,
    services: [MyService],
    reflection: true,
    cors: true,
    allowed_origins: "*"
  ```

  ## Usage in a Plug Pipeline / Phoenix Router

  ```elixir
  # In your router or endpoint
  plug SimpleGrpc.Plug.GrpcWeb, cors: true
  plug SimpleGrpc.Plug, services: [MyService]
  ```

  ## Options

  - `:cors` (boolean) - Enable or disable CORS preflight and response headers (default: `true`).
  - `:allowed_origins` - Allowed CORS origins (`"*"` or list of strings, default: `"*"`).
  - `:allowed_headers` - Allowed request headers or `:any` (default: `:any`).
  - `:exposed_headers` - List of response headers exposed to JS (default includes `grpc-status`, `grpc-message`, `grpc-status-details-bin`, etc.).
  - `:max_age` - CORS max-age in seconds (default: `86400`).
  - `:services`, `:server`, `:reflection`, `:health`, `:interceptors` - Forwarded directly to `SimpleGrpc.Plug` if hosting services.
  """

  @behaviour Plug

  import Plug.Conn
  alias SimpleGrpc.GrpcWeb

  @impl true
  def init(opts) do
    opts_map = Enum.into(opts, %{})
    cors_enabled = Map.get(opts_map, :cors, true)

    cors_opts =
      [
        allowed_origins: Map.get(opts_map, :allowed_origins, "*"),
        allowed_headers: Map.get(opts_map, :allowed_headers, :any),
        exposed_headers: Map.get(opts_map, :exposed_headers, nil),
        max_age: Map.get(opts_map, :max_age, 86400)
      ]
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)

    has_services =
      Map.has_key?(opts_map, :services) or
        Map.has_key?(opts_map, :server) or
        Map.has_key?(opts_map, :health) or
        Map.has_key?(opts_map, :reflection)

    plug_state = if has_services, do: SimpleGrpc.Plug.init(opts), else: nil

    %{
      cors_enabled: cors_enabled,
      cors_opts: cors_opts,
      plug_state: plug_state,
      opts: opts
    }
  end

  @impl true
  def call(conn, state) do
    state = normalize_state(state)

    if state.cors_enabled and GrpcWeb.cors_preflight?(conn) do
      conn
      |> GrpcWeb.handle_cors_preflight(state.cors_opts)
      |> halt()
    else
      conn =
        if state.cors_enabled do
          GrpcWeb.put_cors_headers(conn, state.cors_opts)
        else
          conn
        end

      if state.plug_state do
        SimpleGrpc.Plug.call(conn, state.plug_state)
      else
        conn
      end
    end
  end

  defp normalize_state(%{cors_enabled: _} = state), do: state
  defp normalize_state(opts) when is_list(opts), do: init(opts)
  defp normalize_state(_), do: init([])
end
