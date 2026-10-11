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

  # ===========================================================================
  # Rich Error Details (`google.rpc.Status` & `grpc-status-details-bin`)
  # ===========================================================================

  alias SimpleGrpc.RPC.Status, as: RPCStatus
  alias SimpleGrpc.ErrorDetails
  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Schema.WellKnownTypes

  @doc """
  Constructs a new `%SimpleGrpc.RPC.Status{}` struct.
  """
  defdelegate new(code_or_atom), to: RPCStatus
  defdelegate new(code_or_atom, message), to: RPCStatus
  defdelegate new(code_or_atom, message, details), to: RPCStatus

  @doc """
  Serializes a status code, message, and details list into the Base64-encoded binary format
  expected by the standard `grpc-status-details-bin` trailing metadata header.
  """
  def to_status_details_bin(%RPCStatus{code: code, message: message, details: details}) do
    encode_status_details_bin(code, message, details)
  end

  def to_status_details_bin({code_or_atom, message, details}) when is_list(details) do
    code_int = code(code_or_atom)
    encode_status_details_bin(code_int, message, details)
  end

  def to_status_details_bin(%{code: code, message: message, details: details}) when is_list(details) do
    code_int = code(code)
    encode_status_details_bin(code_int, message, details)
  end

  def to_status_details_bin(other) do
    status = to_rpc_status(other)
    encode_status_details_bin(status.code, status.message, status.details)
  end

  @doc """
  Decodes a `grpc-status-details-bin` header (Base64 string or raw binary) into a `%SimpleGrpc.RPC.Status{}` struct.
  """
  def from_status_details_bin(nil), do: {:error, :nil_payload}
  def from_status_details_bin(""), do: {:error, :empty_payload}

  def from_status_details_bin(bin_or_base64) when is_binary(bin_or_base64) do
    raw_bytes =
      case Base.decode64(bin_or_base64) do
        {:ok, decoded} -> decoded
        :error -> bin_or_base64
      end

    schema_ctx = WellKnownTypes.unified_schema()

    case Protobuf.decode("google.rpc.Status", raw_bytes, schema_ctx) do
      {:ok, %{code: code, message: message} = status_map} ->
        raw_details = Map.get(status_map, :details, [])
        unpacked_details = ErrorDetails.unpack_all(raw_details)

        {:ok,
         %RPCStatus{
           code: code || 0,
           status_code: atom(code || 0),
           message: message || "",
           details: unpacked_details
         }}

      {:error, _code, reason} ->
        {:error, "Failed to decode google.rpc.Status: #{reason}"}

      other ->
        {:error, "Unexpected status decode result: #{inspect(other)}"}
    end
  end

  def from_status_details_bin(_), do: {:error, :invalid_argument}

  @doc """
  Extracts gRPC status code, message, and rich status (`%SimpleGrpc.RPC.Status{}` or `nil`)
  from a list or map of headers/trailers.
  """
  def extract_status(headers_or_trailers) when is_list(headers_or_trailers) do
    status_str =
      Enum.find_value(headers_or_trailers, fn {k, v} ->
        if String.downcase(to_string(k)) == "grpc-status", do: to_string(v)
      end)

    msg_str =
      Enum.find_value(headers_or_trailers, "", fn {k, v} ->
        if String.downcase(to_string(k)) == "grpc-message", do: URI.decode(to_string(v))
      end)

    status_code =
      case status_str do
        nil ->
          0

        val ->
          case Integer.parse(val) do
            {int, _} -> int
            :error -> 2
          end
      end

    rich_status =
      case Enum.find_value(headers_or_trailers, fn {k, v} ->
             if String.downcase(to_string(k)) == "grpc-status-details-bin", do: to_string(v)
           end) do
        nil ->
          nil

        b64_str ->
          case from_status_details_bin(b64_str) do
            {:ok, status} -> status
            _ -> nil
          end
      end

    {status_code, msg_str, rich_status}
  end

  def extract_status(map) when is_map(map) do
    extract_status(Map.to_list(map))
  end

  @doc """
  Extracts unpacked error details from headers, trailers, or an error tuple/struct.
  Returns a list of error details, or `[]` if none present.
  """
  def extract_details(%RPCStatus{details: details}), do: details
  def extract_details({:error, %RPCStatus{details: details}}), do: details
  def extract_details({:error, _code, _msg, %RPCStatus{details: details}}), do: details
  def extract_details({:error, _code, _msg, details}) when is_list(details), do: ErrorDetails.unpack_all(details)

  def extract_details(headers_or_trailers) when is_list(headers_or_trailers) or is_map(headers_or_trailers) do
    case extract_status(headers_or_trailers) do
      {_code, _msg, %RPCStatus{details: details}} -> details
      _ -> []
    end
  end

  def extract_details(_), do: []

  @doc """
  Converts any error return representation into a canonical `%SimpleGrpc.RPC.Status{}` struct.
  """
  def to_rpc_status(%RPCStatus{} = status), do: status

  def to_rpc_status({:error, %RPCStatus{} = status}), do: status

  def to_rpc_status({:error, code, msg, details}) when is_list(details) do
    RPCStatus.new(code, msg, details)
  end

  def to_rpc_status({:error, code, msg}) do
    RPCStatus.new(code, msg, [])
  end

  def to_rpc_status({code, msg, details}) when is_list(details) do
    RPCStatus.new(code, msg, details)
  end

  def to_rpc_status({code, msg}) do
    RPCStatus.new(code, msg, [])
  end

  def to_rpc_status(code_or_atom) do
    RPCStatus.new(code_or_atom, "", [])
  end

  defp encode_status_details_bin(code, message, details) do
    schema_ctx = WellKnownTypes.unified_schema()

    packed_details =
      details
      |> List.wrap()
      |> Enum.map(&ErrorDetails.pack/1)

    status_payload = %{
      code: code || 0,
      message: to_string(message || ""),
      details: packed_details
    }

    encoded_bytes = Protobuf.encode!("google.rpc.Status", status_payload, schema_ctx)
    Base.encode64(encoded_bytes)
  end
end
