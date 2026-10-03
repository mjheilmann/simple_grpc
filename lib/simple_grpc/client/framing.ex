defmodule SimpleGrpc.Client.Framing do
  @moduledoc """
  Client framing module delegating to `SimpleGrpc.Framing`.
  """
  defdelegate frame(payload), to: SimpleGrpc.Framing
  defdelegate unframe(binary), to: SimpleGrpc.Framing
  defdelegate extract_grpc_status(headers_or_trailers), to: SimpleGrpc.Framing
end
