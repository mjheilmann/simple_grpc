defmodule SimpleGrpc.Client.Telemetry do
  @moduledoc """
  `:telemetry` instrumentation helpers for gRPC client invocations.

  Emits standardized client events:
  - `[:simple_grpc, :client, :start]`
  - `[:simple_grpc, :client, :stop]`
  - `[:simple_grpc, :client, :exception]`
  """

  @doc """
  Executes the given function within a client telemetry span.
  """
  def span(metadata, fun) when is_map(metadata) and is_function(fun, 0) do
    start_time = System.monotonic_time()

    start_measurements = %{
      system_time: System.system_time(),
      monotonic_time: start_time
    }

    :telemetry.execute([:simple_grpc, :client, :start], start_measurements, metadata)

    try do
      result = fun.()

      stop_time = System.monotonic_time()
      duration = stop_time - start_time
      stop_measurements = %{duration: duration, monotonic_time: stop_time}

      case result do
        {:ok, response} ->
          stop_metadata =
            metadata
            |> Map.merge(%{
              response: response,
              status: 0,
              grpc_status: 0,
              error: nil,
              grpc_message: ""
            })

          :telemetry.execute([:simple_grpc, :client, :stop], stop_measurements, stop_metadata)
          {:ok, response}

        {:error, status_code, status_msg} ->
          stop_metadata =
            metadata
            |> Map.merge(%{
              response: nil,
              status: status_code,
              grpc_status: status_code,
              error: status_msg,
              grpc_message: status_msg
            })

          :telemetry.execute([:simple_grpc, :client, :stop], stop_measurements, stop_metadata)
          {:error, status_code, status_msg}

        {:error, reason} ->
          error_str = inspect(reason)

          stop_metadata =
            metadata
            |> Map.merge(%{
              response: nil,
              status: 2,
              grpc_status: 2,
              error: error_str,
              grpc_message: error_str
            })

          :telemetry.execute([:simple_grpc, :client, :stop], stop_measurements, stop_metadata)
          {:error, reason}

        other ->
          stop_metadata =
            metadata
            |> Map.merge(%{
              response: other,
              status: 0,
              grpc_status: 0,
              error: nil,
              grpc_message: ""
            })

          :telemetry.execute([:simple_grpc, :client, :stop], stop_measurements, stop_metadata)
          other
      end
    catch
      kind, reason ->
        stop_time = System.monotonic_time()
        duration = stop_time - start_time
        exc_measurements = %{duration: duration, monotonic_time: stop_time}

        exc_metadata =
          metadata
          |> Map.merge(%{
            kind: kind,
            reason: reason,
            stacktrace: __STACKTRACE__
          })

        :telemetry.execute([:simple_grpc, :client, :exception], exc_measurements, exc_metadata)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end
end
