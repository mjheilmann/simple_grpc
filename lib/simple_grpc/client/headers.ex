defmodule SimpleGrpc.Client.Headers do
  @moduledoc """
  Utilities for normalizing, merging, and formatting gRPC client headers and metadata.
  """

  @doc """
  Normalizes headers from maps, keyword lists, or lists of 2-tuples into a list of `{"header-name", "header-value"}` string tuples.
  """
  def normalize(nil), do: []

  def normalize(headers) when is_map(headers) do
    headers
    |> Enum.map(fn {k, v} -> {normalize_key(k), to_string(v)} end)
  end

  def normalize(headers) when is_list(headers) do
    Enum.map(headers, fn
      {k, v} -> {normalize_key(k), to_string(v)}
      other -> {to_string(other), ""}
    end)
  end

  def normalize(_), do: []

  @doc """
  Merges connection-level headers with call-specific headers and applies timeouts if present.
  Call headers take precedence over connection headers.
  """
  def merge(conn_headers, call_headers, opts \\ []) do
    norm_conn = normalize(conn_headers)
    norm_call = normalize(call_headers)

    # Convert to map for deduplication / overriding, preserving case-insensitive lowercase keys
    conn_map = Map.new(norm_conn, fn {k, v} -> {String.downcase(k), {k, v}} end)
    call_map = Map.new(norm_call, fn {k, v} -> {String.downcase(k), {k, v}} end)

    merged_map = Map.merge(conn_map, call_map)

    # Check for compression options
    compress? =
      Keyword.get(opts, :compress, false) == true or
        Keyword.get(opts, :encoding) in ["gzip", :gzip]

    merged_map =
      if compress? and not Map.has_key?(merged_map, "grpc-encoding") do
        Map.put(merged_map, "grpc-encoding", {"grpc-encoding", "gzip"})
      else
        merged_map
      end

    merged_map =
      if not Map.has_key?(merged_map, "grpc-accept-encoding") do
        Map.put(merged_map, "grpc-accept-encoding", {"grpc-accept-encoding", "identity,gzip"})
      else
        merged_map
      end

    # Check for timeout option
    merged_map =
      case Keyword.get(opts, :timeout) do
        nil ->
          merged_map

        timeout ->
          if Map.has_key?(merged_map, "grpc-timeout") do
            merged_map
          else
            Map.put(merged_map, "grpc-timeout", {"grpc-timeout", format_grpc_timeout(timeout)})
          end
      end

    Map.values(merged_map)
  end

  @doc """
  Merges options from `conn.opts` and call `opts`, combining headers and preserving timeout.
  """
  def merge_opts(conn, opts) do
    conn_headers = (conn && conn.opts && Keyword.get(conn.opts, :headers)) || []
    call_headers = Keyword.get(opts, :headers, [])
    merged_headers = merge(conn_headers, call_headers, opts)

    opts
    |> Keyword.put(:headers, merged_headers)
  end

  @doc """
  Formats a timeout value into the standard gRPC `grpc-timeout` header string.
  """
  def format_grpc_timeout(timeout) when is_integer(timeout) and timeout > 0 do
    "#{timeout}m"
  end

  def format_grpc_timeout(timeout) when is_binary(timeout) do
    trimmed = String.trim(timeout)

    if Regex.match?(~r/^\d+[HMSEumn]$/, trimmed) do
      trimmed
    else
      "#{trimmed}m"
    end
  end

  def format_grpc_timeout(_), do: "5000m"

  @doc """
  Parses a `grpc-timeout` header string into milliseconds.
  Supported units:
  - `H` (Hours)
  - `M` (Minutes)
  - `S` (Seconds)
  - `m` (Milliseconds)
  - `u` (Microseconds)
  - `n` (Nanoseconds)

  Returns `{:ok, ms}` or `:error`.
  """
  def parse_grpc_timeout(nil), do: :error
  def parse_grpc_timeout(val) when is_integer(val) and val >= 0, do: {:ok, val}

  def parse_grpc_timeout(val) when is_binary(val) do
    case Regex.run(~r/^(\d+)\s*([HMSumn])$/, String.trim(val)) do
      [_, count_str, unit] ->
        count = String.to_integer(count_str)

        ms =
          case unit do
            "H" -> count * 3_600_000
            "M" -> count * 60_000
            "S" -> count * 1_000
            "m" -> count
            "u" -> max(1, div(count, 1_000))
            "n" -> max(1, div(count, 1_000_000))
          end

        {:ok, ms}

      _ ->
        case Integer.parse(String.trim(val)) do
          {count, ""} -> {:ok, count}
          _ -> :error
        end
    end
  end

  def parse_grpc_timeout(_), do: :error

  defp normalize_key(k) when is_atom(k) do
    k |> to_string() |> String.replace("_", "-") |> String.downcase()
  end

  defp normalize_key(k) when is_binary(k) do
    String.downcase(k)
  end

  defp normalize_key(k), do: to_string(k)
end
