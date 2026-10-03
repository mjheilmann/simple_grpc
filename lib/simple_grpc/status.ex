defmodule SimpleGrpc.Status do
  @moduledoc """
  Standard gRPC status codes and bidirectional atom-to-code mapping.

  | Code | Atom | Description |
  | --- | --- | --- |
  | 0 | `:ok` | Success |
  | 1 | `:cancelled` / `:canceled` | The operation was cancelled |
  | 2 | `:unknown` | Unknown error |
  | 3 | `:invalid_argument` | Client specified an invalid argument |
  | 4 | `:deadline_exceeded` | Deadline expired before operation could complete |
  | 5 | `:not_found` | Requested entity was not found |
  | 6 | `:already_exists` | Entity that a client attempted to create already exists |
  | 7 | `:permission_denied` | Caller does not have permission |
  | 8 | `:resource_exhausted` | Resource has been exhausted (quota, space) |
  | 9 | `:failed_precondition` | System is not in state required for operation |
  | 10 | `:aborted` | Operation was aborted |
  | 11 | `:out_of_range` | Operation was attempted past the valid range |
  | 12 | `:unimplemented` | Operation is not implemented or supported |
  | 13 | `:internal` | Internal errors |
  | 14 | `:unavailable` | Service is currently unavailable |
  | 15 | `:data_loss` | Unrecoverable data loss or corruption |
  | 16 | `:unauthenticated` | Request does not have valid authentication credentials |
  """

  @statuses %{
    ok: 0,
    cancelled: 1,
    canceled: 1,
    unknown: 2,
    invalid_argument: 3,
    deadline_exceeded: 4,
    not_found: 5,
    already_exists: 6,
    permission_denied: 7,
    resource_exhausted: 8,
    failed_precondition: 9,
    aborted: 10,
    out_of_range: 11,
    unimplemented: 12,
    internal: 13,
    unavailable: 14,
    data_loss: 15,
    unauthenticated: 16
  }

  @codes %{
    0 => :ok,
    1 => :cancelled,
    2 => :unknown,
    3 => :invalid_argument,
    4 => :deadline_exceeded,
    5 => :not_found,
    6 => :already_exists,
    7 => :permission_denied,
    8 => :resource_exhausted,
    9 => :failed_precondition,
    10 => :aborted,
    11 => :out_of_range,
    12 => :unimplemented,
    13 => :internal,
    14 => :unavailable,
    15 => :data_loss,
    16 => :unauthenticated
  }

  @doc """
  Converts a status atom or integer to an integer gRPC status code.
  """
  def code(int) when is_integer(int), do: int
  def code(atom) when is_atom(atom), do: Map.get(@statuses, atom, 2)
  def code(_), do: 2

  @doc """
  Converts an integer gRPC status code to its canonical atom representation.
  """
  def atom(code) when is_integer(code), do: Map.get(@codes, code, :unknown)
  def atom(atom) when is_atom(atom), do: atom
  def atom(_), do: :unknown

  @doc """
  Returns true if the code or atom represents success (`:ok` / `0`).
  """
  def ok?(0), do: true
  def ok?(:ok), do: true
  def ok?(_), do: false
end
