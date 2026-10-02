defmodule SimpleGrpc.Client.Conn do
  @moduledoc """
  Represents an active gRPC client connection managed by a transport adapter.
  """

  defstruct [:adapter, :state, :target, :opts]

  @type t :: %__MODULE__{
          adapter: module(),
          state: term(),
          target: term(),
          opts: keyword()
        }
end
