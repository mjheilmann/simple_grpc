defmodule SimpleGrpc.Framing do
  @moduledoc """
  Standard 5-byte length-prefixed gRPC message framing and status parsing.
  Shared across both Client and Server transports.
  """

  alias SimpleGrpc.Compression

  @doc """
  Frames a binary payload with the 5-byte gRPC prefix:
  `<<compressed::8, payload_length::32-big, payload::binary>>`.

  Options:
  - `:compress` (boolean) or `:encoding` (`"gzip"`, `"deflate"`, `"zstd"`, `"identity"`, etc.)
  """
  def frame(payload, opts \\ [])

  def frame(payload, opts) when is_binary(payload) and is_list(opts) do
    encoding =
      case Keyword.get(opts, :encoding) do
        nil ->
          if Keyword.get(opts, :compress, false) == true, do: "gzip", else: "identity"

        enc when is_atom(enc) ->
          to_string(enc)

        enc when is_binary(enc) ->
          enc
      end

    case Compression.compress(payload, encoding) do
      {:ok, compressed_payload, flag} ->
        <<flag::8, byte_size(compressed_payload)::32-big, compressed_payload::binary>>

      {:error, _reason} ->
        # Fallback to uncompressed if compression fails
        <<0::8, byte_size(payload)::32-big, payload::binary>>
    end
  end

  def frame(payload, _opts) when is_binary(payload) do
    <<0::8, byte_size(payload)::32-big, payload::binary>>
  end

  @doc """
  Unframes a 5-byte prefixed gRPC binary message.
  If the message compressed flag is 1, decompresses payload using the specified `:encoding` (default `"gzip"`).
  Returns `{:ok, payload, rest}` or `{:incomplete, binary}` or `{:error, reason}`.
  """
  def unframe(binary, opts \\ [])

  def unframe(
        <<compressed::8, length::32-big, payload::binary-size(length), rest::binary>>,
        opts
      ) do
    case compressed do
      0 ->
        {:ok, payload, rest}

      1 ->
        encoding = Keyword.get(opts, :encoding, "gzip")

        case Compression.decompress(payload, encoding) do
          {:ok, decompressed} ->
            {:ok, decompressed, rest}

          {:error, reason} ->
            {:error, "Failed to decompress gRPC payload (#{inspect(encoding)}): #{reason}"}
        end

      128 ->
        trailers = SimpleGrpc.GrpcWeb.parse_trailers_block(payload)
        {:trailers, trailers, rest}

      other ->
        {:error, "Unsupported compression flag: #{other}"}
    end
  end

  @doc """
  Frames trailers into a gRPC-Web trailer frame (`0x80` flag).
  """
  defdelegate frame_trailers(trailers), to: SimpleGrpc.GrpcWeb
  defdelegate frame_trailers(status_code, status_message, details), to: SimpleGrpc.GrpcWeb

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

  @doc """
  Extracts gRPC status code, message, and rich status (`%SimpleGrpc.RPC.Status{}` or `nil`)
  from a list of headers or trailers.
  """
  defdelegate extract_grpc_status_details(headers_or_trailers),
    to: SimpleGrpc.Status,
    as: :extract_status
end
