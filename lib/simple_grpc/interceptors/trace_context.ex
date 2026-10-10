defmodule SimpleGrpc.Interceptors.TraceContext do
  @moduledoc """
  OpenTelemetry and W3C Trace Context (`traceparent`) propagation interceptor for client and server.

  ## Features
  - Automatic `traceparent` W3C header injection on client outgoing requests.
  - Automatic `traceparent` extraction and child span derivation on incoming server requests.
  - Storage of trace metadata in `ctx.private[:trace_id]` and `ctx.private[:trace_context]`.

  ## Client Usage
      SimpleGrpc.Client.connect("localhost:50051",
        interceptors: [SimpleGrpc.Interceptors.TraceContext]
      )

  ## Server Usage
      plug SimpleGrpc.Plug,
        services: [MyServer],
        interceptors: [SimpleGrpc.Interceptors.TraceContext]
  """

  @behaviour SimpleGrpc.Interceptor

  alias SimpleGrpc.Context
  alias SimpleGrpc.TraceContext

  @impl true
  def intercept(req, ctx, next, opts \\ [])

  def intercept(req, %Context{role: :client} = ctx, next, opts) do
    tc =
      case TraceContext.extract(ctx) do
        {:ok, existing_tc} ->
          if Keyword.get(opts, :force_new, false) do
            TraceContext.generate(opts)
          else
            existing_tc
          end

        :error ->
          TraceContext.generate(opts)
      end

    ctx =
      ctx
      |> TraceContext.inject(tc)
      |> Context.put_private(:trace_id, tc.trace_id)
      |> Context.put_private(:trace_context, tc)

    next.(req, ctx)
  end

  def intercept(req, %Context{} = ctx, next, opts) do
    # Server role (or fallback)
    tc =
      case TraceContext.extract(ctx) do
        {:ok, parent_tc} ->
          TraceContext.child_span(parent_tc, opts)

        :error ->
          if Keyword.get(opts, :generate_if_missing, true) do
            TraceContext.generate(opts)
          else
            nil
          end
      end

    ctx =
      if tc do
        ctx
        |> Context.put_private(:trace_id, tc.trace_id)
        |> Context.put_private(:trace_context, tc)
      else
        ctx
      end

    next.(req, ctx)
  end
end

defmodule SimpleGrpc.Interceptors.OpenTelemetry do
  @moduledoc """
  Alias for `SimpleGrpc.Interceptors.TraceContext`.
  """
  @behaviour SimpleGrpc.Interceptor

  defdelegate intercept(req, ctx, next), to: SimpleGrpc.Interceptors.TraceContext
  defdelegate intercept(req, ctx, next, opts), to: SimpleGrpc.Interceptors.TraceContext
end
