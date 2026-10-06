defmodule SimpleGrpc.Framing do
  @moduledoc """
  Standard 5-byte length-prefixed gRPC message framing and status parsing.
  Shared across both Client and Server transports.
  """

  @doc """
  Frames a binary payload with the 5-byte gRPC prefix:
  `<<compressed::8, payload_length::32-big, payload::binary>>`.

  Options:
  - `:compress` (boolean) or `:encoding` (`"gzip"` or `:gzip`) - compresses payload with gzip and sets compressed flag to 1.
  """
  def frame(payload, opts \\ [])

  def frame(payload, opts) when is_binary(payload) and is_list(opts) do
    compress? =
      Keyword.get(opts, :compress, false) == true or
        Keyword.get(opts, :encoding) in ["gzip", :gzip]

    if compress? do
      compressed = :zlib.gzip(payload)
      <<1::8, byte_size(compressed)::32-big, compressed::binary>>
    else
      <<0::8, byte_size(payload)::32-big, payload::binary>>
    end
  end

  def frame(payload, _opts) when is_binary(payload) do
    <<0::8, byte_size(payload)::32-big, payload::binary>>
  end

  @doc """
  Unframes a 5-byte prefixed gRPC binary message.
  If the message compressed flag is 1, decompresses payload using gzip.
  Returns `{:ok, payload, rest}` or `{:incomplete, binary}` or `{:error, reason}`.
  """
  def unframe(binary, opts \\ [])

  def unframe(
        <<compressed::8, length::32-big, payload::binary-size(length), rest::binary>>,
        _opts
      ) do
    case compressed do
      0 ->
        {:ok, payload, rest}

      1 ->
        try do
          decompressed = :zlib.gunzip(payload)
          {:ok, decompressed, rest}
        rescue
          e ->
            {:error, "Failed to decompress gRPC payload: #{Exception.message(e)}"}
        end

      other ->
        {:error, "Unsupported compression flag: #{other}"}
    end
  end

  def unframe(binary, _opts) when is_binary(binary) do
    {:incomplete, binary}
  end

  @doc """
  Extracts gRPC status code and status message from a list of headers or trailers.
  Defaults to status 0 (`:ok`) if `grpc-status` is not explicitly present.
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
        nil ->
          0

        val ->
          case Integer.parse(val) do
            {int, _} -> int
            :error -> 2
          end
      end

    {status_code, msg_str}
  end
end
