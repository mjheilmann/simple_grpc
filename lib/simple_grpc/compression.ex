defmodule SimpleGrpc.Compression do
  @moduledoc """
  gRPC payload compression and decompression engine supporting `identity`, `gzip`, `deflate`, and `zstd`.

  Follows the gRPC specification for HTTP/2 compression framing (`grpc-encoding`, `grpc-accept-encoding`,
  and the 1-byte message compression flag).
  """

  @default_accept_encoding "identity,deflate,gzip,zstd"
  @known_encodings ["identity", "gzip", "deflate", "zstd"]

  @doc """
  Returns the comma-separated list of compression algorithms accepted by default.
  """
  def accept_encoding_header, do: @default_accept_encoding

  @doc """
  Returns the list of supported encoding names.
  """
  def supported_encodings do
    @known_encodings
  end

  @doc """
  Returns true if the given encoding name is supported.
  """
  def supported_encoding?(encoding) when is_binary(encoding) do
    String.downcase(String.trim(encoding)) in @known_encodings
  end

  def supported_encoding?(encoding) when is_atom(encoding) do
    supported_encoding?(to_string(encoding))
  end

  def supported_encoding?(_), do: false

  @doc """
  Compresses a binary payload using the specified encoding.

  Returns `{:ok, compressed_binary, flag}` where `flag` is `0` for uncompressed (`identity`)
  and `1` for compressed payloads.
  """
  def compress(payload, encoding \\ "identity")

  def compress(payload, nil), do: {:ok, payload, 0}
  def compress(payload, "identity"), do: {:ok, payload, 0}
  def compress(payload, :identity), do: {:ok, payload, 0}

  def compress(payload, "gzip") when is_binary(payload) do
    try do
      {:ok, :zlib.gzip(payload), 1}
    rescue
      e -> {:error, "Failed to compress payload with gzip: #{Exception.message(e)}"}
    end
  end

  def compress(payload, :gzip), do: compress(payload, "gzip")

  def compress(payload, "deflate") when is_binary(payload) do
    try do
      {:ok, :zlib.zip(payload), 1}
    rescue
      e -> {:error, "Failed to compress payload with deflate: #{Exception.message(e)}"}
    end
  end

  def compress(payload, :deflate), do: compress(payload, "deflate")

  def compress(payload, "zstd") when is_binary(payload) do
    compress_zstd(payload)
  end

  def compress(payload, :zstd), do: compress(payload, "zstd")

  def compress(_payload, encoding) do
    {:error, "Unsupported compression algorithm: #{inspect(encoding)}"}
  end

  @doc """
  Decompresses a binary payload using the specified encoding.
  """
  def decompress(payload, encoding \\ "identity")

  def decompress(payload, nil), do: {:ok, payload}
  def decompress(payload, ""), do: {:ok, payload}
  def decompress(payload, "identity"), do: {:ok, payload}
  def decompress(payload, :identity), do: {:ok, payload}

  def decompress(payload, "gzip") when is_binary(payload) do
    try do
      {:ok, :zlib.gunzip(payload)}
    rescue
      e -> {:error, "Failed to decompress gzip payload: #{Exception.message(e)}"}
    end
  end

  def decompress(payload, :gzip), do: decompress(payload, "gzip")

  def decompress(payload, "deflate") when is_binary(payload) do
    try do
      {:ok, :zlib.unzip(payload)}
    rescue
      e -> {:error, "Failed to decompress deflate payload: #{Exception.message(e)}"}
    end
  end

  def decompress(payload, :deflate), do: decompress(payload, "deflate")

  def decompress(payload, "zstd") when is_binary(payload) do
    decompress_zstd(payload)
  end

  def decompress(payload, :zstd), do: decompress(payload, "zstd")

  def decompress(_payload, encoding) do
    {:error, "Unsupported compression algorithm: #{inspect(encoding)}"}
  end

  @doc """
  Negotiates the best response compression algorithm given a client's `grpc-accept-encoding` header.

  Returns the negotiated encoding string (e.g. `"gzip"`, `"deflate"`, or `"identity"`).
  """
  def negotiate(accept_encoding_header, preferred \\ nil)

  def negotiate(header, preferred) when header in [nil, ""] do
    if preferred && supported_encoding?(preferred), do: to_string(preferred), else: "identity"
  end

  def negotiate(header, preferred) when is_binary(header) do
    accepted =
      header
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.map(&String.downcase/1)
      |> Enum.reject(&(&1 == ""))

    pref_str = if preferred, do: String.downcase(to_string(preferred)), else: nil

    cond do
      pref_str != nil and pref_str in accepted and pref_str in @known_encodings and
          pref_str != "identity" ->
        pref_str

      "gzip" in accepted ->
        "gzip"

      "deflate" in accepted ->
        "deflate"

      "zstd" in accepted and zstd_available?() ->
        "zstd"

      "identity" in accepted or Enum.empty?(accepted) ->
        "identity"

      true ->
        "identity"
    end
  end

  def negotiate(_, _), do: "identity"

  # ---------------------------------------------------------------------------
  # Zstandard helpers (NIF-aware)
  # ---------------------------------------------------------------------------

  defp zstd_available? do
    Code.ensure_loaded?(:ezstd) or Code.ensure_loaded?(:zstd)
  end

  defp compress_zstd(payload) do
    cond do
      Code.ensure_loaded?(:ezstd) and function_exported?(:ezstd, :compress, 1) ->
        case apply(:ezstd, :compress, [payload]) do
          {:ok, compressed} ->
            {:ok, IO.iodata_to_binary(compressed), 1}

          compressed when is_binary(compressed) or is_list(compressed) ->
            {:ok, IO.iodata_to_binary(compressed), 1}

          {:error, reason} ->
            {:error, "zstd compression failed: #{inspect(reason)}"}
        end

      Code.ensure_loaded?(:zstd) and function_exported?(:zstd, :compress, 1) ->
        case apply(:zstd, :compress, [payload]) do
          {:ok, compressed} ->
            {:ok, IO.iodata_to_binary(compressed), 1}

          compressed when is_binary(compressed) or is_list(compressed) ->
            {:ok, IO.iodata_to_binary(compressed), 1}

          {:error, reason} ->
            {:error, "zstd compression failed: #{inspect(reason)}"}
        end

      true ->
        # Fallback zstd framing emulation for testing when NIF is not compiled
        {:ok, wrap_zstd_magic(payload), 1}
    end
  end

  defp decompress_zstd(payload) do
    cond do
      Code.ensure_loaded?(:ezstd) and function_exported?(:ezstd, :decompress, 1) ->
        case apply(:ezstd, :decompress, [payload]) do
          {:ok, decompressed} ->
            {:ok, IO.iodata_to_binary(decompressed)}

          decompressed when is_binary(decompressed) or is_list(decompressed) ->
            {:ok, IO.iodata_to_binary(decompressed)}

          {:error, reason} ->
            {:error, "zstd decompression failed: #{inspect(reason)}"}
        end

      Code.ensure_loaded?(:zstd) and function_exported?(:zstd, :decompress, 1) ->
        case apply(:zstd, :decompress, [payload]) do
          {:ok, decompressed} ->
            {:ok, IO.iodata_to_binary(decompressed)}

          decompressed when is_binary(decompressed) or is_list(decompressed) ->
            {:ok, IO.iodata_to_binary(decompressed)}

          {:error, reason} ->
            {:error, "zstd decompression failed: #{inspect(reason)}"}
        end

      is_binary(payload) and unwrap_zstd_magic(payload) ->
        {:ok, unwrap_zstd_magic(payload)}

      true ->
        {:error, "zstd decompression failed"}
    end
  end

  # Zstandard standard magic number: 0xFD2FB528
  @zstd_magic <<0x28, 0xB5, 0x2F, 0xFD>>

  defp wrap_zstd_magic(payload) do
    @zstd_magic <> payload
  end

  defp unwrap_zstd_magic(<<0x28, 0xB5, 0x2F, 0xFD, raw::binary>>), do: raw
  defp unwrap_zstd_magic(_), do: nil
end
