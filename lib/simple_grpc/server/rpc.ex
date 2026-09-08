defmodule SimpleGrpc.Server.RPC do
  @moduledoc """
  Backwards compatibility module aliasing SimpleGrpc.Schema.RPC.
  """
  alias SimpleGrpc.Schema.RPC, as: SchemaRPC

  defstruct [:name, :path, :input, :output, :request_stream, :response_stream, :options]

  @type t :: SchemaRPC.t()

  defdelegate parse(ast), to: SchemaRPC
end
