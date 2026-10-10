defmodule SimpleGrpc.Interceptors.Logger do
  @moduledoc """
  Structured logging interceptor for gRPC client and server pipelines.
  """

  @behaviour SimpleGrpc.Interceptor

  require Logger
  alias SimpleGrpc.Context

  @impl true
  def intercept(req, %Context{} = ctx, next, opts \\ []) do
    level = Keyword.get(opts, :level, :info)
    start_time = System.monotonic_time()

    role_prefix =
      case ctx.role do
        :client -> "[SimpleGrpc.Client]"
        :server -> "[SimpleGrpc.Server]"
        _ -> "[SimpleGrpc]"
      end

    trace_id_str =
      case Context.get_private(ctx, :trace_id) do
        nil -> ""
        tid -> " (trace_id=#{tid})"
      end

    try do
      result = next.(req, ctx)
      duration_us = System.convert_time_unit(System.monotonic_time() - start_time, :native, :microsecond)
      duration_ms = Float.round(duration_us / 1000, 2)

      status_info = format_result_status(result)

      Logger.log(
        level,
        "#{role_prefix} #{ctx.path}#{trace_id_str} -> #{status_info} in #{duration_ms}ms"
      )

      result
    catch
      kind, reason ->
        duration_us = System.convert_time_unit(System.monotonic_time() - start_time, :native, :microsecond)
        duration_ms = Float.round(duration_us / 1000, 2)

        Logger.error(
          "#{role_prefix} #{ctx.path}#{trace_id_str} -> uncaught #{kind} #{inspect(reason)} in #{duration_ms}ms"
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp format_result_status({:ok, _}), do: "OK (0)"
  defp format_result_status({:error, code, msg}), do: "ERROR #{code}: #{msg}"
  defp format_result_status({:error, reason}), do: "ERROR: #{inspect(reason)}"
  defp format_result_status(_), do: "OK (0)"
end
