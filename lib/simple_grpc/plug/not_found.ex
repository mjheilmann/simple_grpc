defmodule SimpleGrpc.Plug.NotFound do
  @moduledoc """
  The last plug of a pipeline: answers whatever nothing else claimed.

    * a gRPC request (`content-type: application/grpc...`) gets `UNIMPLEMENTED`
      ("unknown service or method"), as a gRPC client expects;
    * a `POST` to a gRPC-shaped path (`/<service>/<method>`) with another content type gets
      HTTP 415;
    * anything else gets HTTP 404.

  Without it, a request no service plug claims falls out of the pipeline with no gRPC-shaped
  response.
  """

  @behaviour Plug

  import Plug.Conn

  alias SimpleGrpc.Call

  @impl true
  def init(opts) do
    %{
      unmatched: :unimplemented,
      interceptors: [],
      cors_opts: if(Keyword.get(opts, :cors, true), do: [allowed_origins: "*"])
    }
  end

  @impl true
  def call(conn, state) do
    conn =
      cond do
        grpc?(conn) -> Call.serve(conn, :not_found, state)
        conn.method == "POST" and grpc_shaped_path?(conn.request_path) -> send_resp(conn, 415, "")
        true -> send_resp(conn, 404, "")
      end

    halt(conn)
  end

  defp grpc?(conn) do
    conn
    |> get_req_header("content-type")
    |> Enum.any?(&String.starts_with?(&1, "application/grpc"))
  end

  defp grpc_shaped_path?(path),
    do: match?(["", service, method] when service != "" and method != "", String.split(path, "/"))
end
