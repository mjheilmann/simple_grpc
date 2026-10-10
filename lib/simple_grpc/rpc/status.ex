defmodule SimpleGrpc.RPC.Status do
  @moduledoc """
  Rich gRPC Error Model corresponding to `google.rpc.Status`.

  Represents logical error conditions containing an error code, message,
  and a list of error details (`google.protobuf.Any` messages containing standard details like
  `ErrorInfo`, `BadRequest`, `RetryInfo`, `LocalizedMessage`, `DebugInfo`, etc.).
  """

  alias SimpleGrpc.Status

  @type t :: %__MODULE__{
          code: integer(),
          message: String.t(),
          details: list(),
          status_code: atom()
        }

  defstruct code: 0,
            message: "",
            details: [],
            status_code: :ok

  @doc """
  Constructs a new `%SimpleGrpc.RPC.Status{}` struct.

  ## Examples
      iex> SimpleGrpc.RPC.Status.new(:invalid_argument, "Invalid user ID")
      %SimpleGrpc.RPC.Status{code: 3, message: "Invalid user ID", status_code: :invalid_argument, details: []}

      iex> SimpleGrpc.RPC.Status.new(3, "Invalid user ID", [SimpleGrpc.ErrorDetails.bad_request([%{field: "user_id", description: "must be positive"}])])
  """
  def new(code_or_atom, message \\ "", details \\ [])

  def new(%__MODULE__{} = status, _message, _details), do: status

  def new(code_or_atom, message, details) do
    code_int = Status.code(code_or_atom)
    code_atom = Status.atom(code_int)

    %__MODULE__{
      code: code_int,
      status_code: code_atom,
      message: to_string(message || ""),
      details: List.wrap(details)
    }
  end

  @doc """
  Adds an error detail item to the status.
  """
  def add_detail(%__MODULE__{details: details} = status, detail) do
    %{status | details: details ++ [detail]}
  end
end
