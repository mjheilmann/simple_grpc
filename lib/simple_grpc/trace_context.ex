defmodule SimpleGrpc.TraceContext do
  @moduledoc """
  W3C Trace Context (`traceparent` and `tracestate`) parser, generator, and propagator.

  Follows the W3C Trace Context Level 1 specification:
  - Header: `traceparent` (`00-${trace_id}-${parent_id}-${trace_flags}`)
  - Header: `tracestate` (opaque vendor-specific routing state)
  """

  alias SimpleGrpc.Context

  defstruct version: "00",
            trace_id: nil,
            span_id: nil,
            parent_id: nil,
            flags: "01",
            sampled?: true,
            tracestate: nil

  @type t :: %__MODULE__{
          version: String.t(),
          trace_id: String.t(),
          span_id: String.t(),
          parent_id: String.t() | nil,
          flags: String.t(),
          sampled?: boolean(),
          tracestate: String.t() | nil
        }

  @invalid_trace_id "00000000000000000000000000000000"
  @invalid_span_id "0000000000000000"

  @doc """
  Generates a new root `%SimpleGrpc.TraceContext{}` with random trace ID and span ID.
  """
  def generate(opts \\ []) do
    trace_id = Keyword.get_lazy(opts, :trace_id, fn -> random_hex(16) end)
    span_id = Keyword.get_lazy(opts, :span_id, fn -> random_hex(8) end)
    sampled? = Keyword.get(opts, :sampled, true)
    flags = if sampled?, do: "01", else: "00"
    tracestate = Keyword.get(opts, :tracestate)

    %__MODULE__{
      version: "00",
      trace_id: trace_id,
      span_id: span_id,
      parent_id: nil,
      flags: flags,
      sampled?: sampled?,
      tracestate: tracestate
    }
  end

  @doc """
  Derives a child span context from an existing parent `%SimpleGrpc.TraceContext{}`.
  """
  def child_span(%__MODULE__{} = parent, opts \\ []) do
    new_span_id = Keyword.get_lazy(opts, :span_id, fn -> random_hex(8) end)

    %__MODULE__{
      version: parent.version,
      trace_id: parent.trace_id,
      span_id: new_span_id,
      parent_id: parent.span_id,
      flags: parent.flags,
      sampled?: parent.sampled?,
      tracestate: parent.tracestate
    }
  end

  @doc """
  Parses a W3C `traceparent` header string into a `%SimpleGrpc.TraceContext{}` struct.
  """
  def parse(nil), do: :error

  def parse(traceparent) when is_binary(traceparent) do
    trimmed = String.trim(traceparent)

    case Regex.run(
           ~r/^([0-9a-fA-F]{2})-([0-9a-fA-F]{32})-([0-9a-fA-F]{16})-([0-9a-fA-F]{2})$/,
           trimmed
         ) do
      [_, version, trace_id, span_id, flags] ->
        trace_id = String.downcase(trace_id)
        span_id = String.downcase(span_id)
        flags = String.downcase(flags)

        if trace_id == @invalid_trace_id or span_id == @invalid_span_id do
          :error
        else
          sampled? = flags_sampled?(flags)

          {:ok,
           %__MODULE__{
             version: version,
             trace_id: trace_id,
             span_id: span_id,
             parent_id: nil,
             flags: flags,
             sampled?: sampled?
           }}
        end

      _ ->
        :error
    end
  end

  def parse(_), do: :error

  @doc """
  Formats a `%SimpleGrpc.TraceContext{}` struct into a W3C `traceparent` string.
  """
  def format(%__MODULE__{version: version, trace_id: trace_id, span_id: span_id, flags: flags}) do
    "#{version || "00"}-#{trace_id}-#{span_id}-#{flags || "01"}"
  end

  @doc """
  Extracts trace context from a `%SimpleGrpc.Context{}`, `%Plug.Conn{}`, map, or list of headers.
  """
  def extract(%Context{headers: headers}) do
    extract_from_headers(headers)
  end

  def extract(%Plug.Conn{} = conn) do
    extract_from_headers(conn.req_headers)
  end

  def extract(headers) when is_list(headers) or is_map(headers) do
    extract_from_headers(headers)
  end

  def extract(_), do: :error

  @doc """
  Injects trace context (`traceparent` and `tracestate`) into a `%SimpleGrpc.Context{}` or list of headers.
  """
  def inject(%Context{} = ctx, %__MODULE__{} = tc) do
    ctx = Context.put_header(ctx, "traceparent", format(tc))

    if tc.tracestate && tc.tracestate != "" do
      Context.put_header(ctx, "tracestate", tc.tracestate)
    else
      ctx
    end
  end

  def inject(headers, %__MODULE__{} = tc) when is_list(headers) do
    tp = {"traceparent", format(tc)}

    headers_without_tp =
      Enum.reject(headers, fn
        {k, _v} -> String.downcase(to_string(k)) in ["traceparent", "tracestate"]
        _ -> false
      end)

    if tc.tracestate && tc.tracestate != "" do
      headers_without_tp ++ [tp, {"tracestate", tc.tracestate}]
    else
      headers_without_tp ++ [tp]
    end
  end

  defp extract_from_headers(headers) when is_list(headers) do
    traceparent_val =
      Enum.find_value(headers, fn
        {k, v} ->
          if String.downcase(to_string(k)) == "traceparent", do: to_string(v)

        _ ->
          nil
      end)

    tracestate_val =
      Enum.find_value(headers, fn
        {k, v} ->
          if String.downcase(to_string(k)) == "tracestate", do: to_string(v)

        _ ->
          nil
      end)

    case parse(traceparent_val) do
      {:ok, tc} ->
        {:ok, %{tc | tracestate: tracestate_val}}

      :error ->
        :error
    end
  end

  defp extract_from_headers(headers) when is_map(headers) do
    traceparent_val =
      Enum.find_value(headers, fn {k, v} ->
        if String.downcase(to_string(k)) == "traceparent", do: to_string(v)
      end)

    tracestate_val =
      Enum.find_value(headers, fn {k, v} ->
        if String.downcase(to_string(k)) == "tracestate", do: to_string(v)
      end)

    case parse(traceparent_val) do
      {:ok, tc} ->
        {:ok, %{tc | tracestate: tracestate_val}}

      :error ->
        :error
    end
  end

  defp flags_sampled?(flags) when is_binary(flags) do
    case Integer.parse(flags, 16) do
      {int, ""} -> Bitwise.band(int, 1) == 1
      _ -> true
    end
  end

  defp random_hex(num_bytes) do
    :crypto.strong_rand_bytes(num_bytes) |> Base.encode16(case: :lower)
  end
end
