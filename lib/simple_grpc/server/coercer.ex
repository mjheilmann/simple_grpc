defmodule SimpleGrpc.Server.Coercer do
  @moduledoc """
  Server coercer delegating to `SimpleGrpc.Coercer`.
  """
  defdelegate coerce(attrs, message_module), to: SimpleGrpc.Coercer
end
