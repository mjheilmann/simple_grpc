defmodule SimpleGrpc.Plug.Service do
  @moduledoc """
  Serves one gRPC service, and nothing else.

      plug SimpleGrpc.Plug.Service, server: MyApp.GreeterServer
      plug SimpleGrpc.Plug.Service, server: MyApp.AdminServer
      plug SimpleGrpc.Plug.NotFound

  The plug claims requests whose path starts with the service's prefix (`/pkg.Greeter/`):

    * a known method is handled and the conn is halted;
    * an unknown method under the prefix is answered with `UNIMPLEMENTED` and halted;
    * any other path is returned untouched, so the next plug can claim it.

  Its routes come from the server module alone and are built once per module, then reused. No
  plug knows about any other, so several of these plugs (and pipelines, and ports) are
  independent of each other.

  ## Options

    * `:server` (required) - a module that uses `SimpleGrpc.Server`
    * `:interceptors` - interceptors for this service, run before the ones the module declares
    * `:cors` / `:allowed_origins` - CORS for browser (gRPC-Web) clients, as for `SimpleGrpc.Plug`
      (on by default, any origin)
  """

  @behaviour Plug

  import Plug.Conn

  alias SimpleGrpc.Call
  alias SimpleGrpc.GrpcWeb

  @impl true
  def init(opts) do
    server =
      Keyword.get(opts, :server) ||
        raise ArgumentError, "SimpleGrpc.Plug.Service requires a :server option"

    cors_opts =
      case Keyword.get(opts, :cors, true) do
        false -> nil
        true -> [allowed_origins: Keyword.get(opts, :allowed_origins, "*")]
        cors when is_list(cors) -> cors
      end

    # Validate now so a bad `:server` fails at boot or compile time, not on the first request.
    SimpleGrpc.Service.validate!(server)

    # The state must stay escapable (`Plug.Builder` runs `init/1` at compile time), so it holds
    # no route table: routes are fetched per request from `SimpleGrpc.Service.cached_routes/1`.
    %{
      server: server,
      prefix: server.__grpc_service_path__() <> "/",
      unmatched: :unimplemented,
      interceptors: List.wrap(Keyword.get(opts, :interceptors, [])),
      cors_opts: cors_opts
    }
  end

  @impl true
  def call(%Plug.Conn{request_path: path} = conn, %{prefix: prefix} = state) do
    if String.starts_with?(path, prefix) do
      route_lookup =
        case Map.fetch(SimpleGrpc.Service.cached_routes(state.server), path) do
          {:ok, route} -> {:ok, route}
          :error -> :not_found
        end

      conn
      |> serve(route_lookup, state)
      |> halt()
    else
      conn
    end
  end

  defp serve(conn, route_lookup, state) do
    if state.cors_opts && GrpcWeb.cors_preflight?(conn) do
      GrpcWeb.handle_cors_preflight(conn, state.cors_opts)
    else
      Call.serve(conn, route_lookup, state)
    end
  end
end
