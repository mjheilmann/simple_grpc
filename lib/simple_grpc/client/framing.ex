defmodule SimpleGrpc.Client.Framing do
  @moduledoc """
  Handles standard 5-byte gRPC message framing and header/trailer parsing for client requests and responses.
  """

  @doc """
  Frames a binary payload with the 5-byte gRPC prefix:
  `<<0::8, payload_length::32-big, payload::binary>>`.
  """
  def frame(payload) when is_binary(payload) do
    <<0::8, byte_size(payload)::32-big, payload::binary>>
  end

  @doc """
  Unframes a 5-byte prefixed gRPC binary message.
  Returns `{:ok, payload, rest}` or `{:incomplete, binary}` or `{:error, reason}`.
  """
  def unframe(<<_compressed::8, length::32-big, payload::binary-size(length), rest::binary>>) do
    {:ok, payload, rest}
  end

  def unframe(binary) when is_binary(binary) do
    {:incomplete, binary}
  end

  @doc """
  Extracts gRPC status code and status message from a list of headers or trailers.
  Defaults to status 0 (OK) if `grpc-status` is not explicitly present.
  """
  def extract_grpc_status(headers_or_trailers) when is_list(headers_or_trailers) do
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
        nil -> 0
        val ->
          case Integer.parse(val) do
            {int, _} -> int
            :error -> 2
          end
      end

    {status_code, msg_str}
  end
end
