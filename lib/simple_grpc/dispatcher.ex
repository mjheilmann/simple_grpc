defmodule SimpleGrpc.Dispatcher do
  @moduledoc """
  Dispatches a decoded gRPC request to its registered handler (Module, MFA, Lambda, or PID).
  """

  @doc """
  Executes the handler associated with the given route.
  """
  def dispatch(%{handler: handler, rpc_name: rpc_name}, req, stream \\ nil) do
    invoke_handler(handler, rpc_name, req, stream)
  end

  @doc """
  Invokes a handler and normalizes the return value into `{:ok, result}` or `{:error, code, message}`.
  Exceptions are not rescued here — they propagate to the Plug layer for gRPC status mapping.
  """
  def invoke_handler(handler, rpc_name, req, stream) do
    raw_result = execute_handler(handler, rpc_name, req, stream)

    normalize_result(raw_result)
  end

  defp execute_handler(handler, _rpc_name, req, stream) when is_function(handler, 2) do
    handler.(req, stream)
  end

  defp execute_handler(handler, _rpc_name, req, _stream) when is_function(handler, 1) do
    handler.(req)
  end

  defp execute_handler({mod, fun}, _rpc_name, req, stream) when is_atom(mod) and is_atom(fun) do
    if function_exported?(mod, fun, 2) do
      apply(mod, fun, [req, stream])
    else
      apply(mod, fun, [req])
    end
  end

  defp execute_handler(module, rpc_name, req, stream) when is_atom(module) do
    snake_atom = String.to_atom(Macro.underscore(rpc_name))
    orig_atom = String.to_atom(rpc_name)

    cond do
      function_exported?(module, snake_atom, 2) ->
        apply(module, snake_atom, [req, stream])

      function_exported?(module, snake_atom, 1) ->
        apply(module, snake_atom, [req])

      function_exported?(module, orig_atom, 2) ->
        apply(module, orig_atom, [req, stream])

      function_exported?(module, orig_atom, 1) ->
        apply(module, orig_atom, [req])

      function_exported?(module, :__handlers__, 0) ->
        case SimpleGrpc.Server.find_handler_in(module.__handlers__(), rpc_name) do
          nil ->
            {:error, 12, "No handler found for #{rpc_name} in #{inspect(module)}"}

          h ->
            execute_handler(h, rpc_name, req, stream)
        end

      true ->
        {:error, 12, "No handler function found for #{rpc_name} in #{inspect(module)}"}
    end
  end

  defp execute_handler(handler, rpc_name, _req, _stream) do
    {:error, 12, "Unsupported handler #{inspect(handler)} for RPC #{inspect(rpc_name)}"}
  end

  defp normalize_result({:ok, _} = ok), do: ok

  defp normalize_result({:error, %SimpleGrpc.RPC.Status{} = status}),
    do: {:error, status}

  defp normalize_result({:error, code, msg, details}) when is_integer(code) and is_binary(msg) and is_list(details),
    do: {:error, code, msg, details}

  defp normalize_result({:error, code, msg, details}) when is_atom(code) and is_binary(msg) and is_list(details),
    do: {:error, SimpleGrpc.Status.code(code), msg, details}

  defp normalize_result({:error, code, msg}) when is_integer(code) and is_binary(msg),
    do: {:error, code, msg}

  defp normalize_result({:error, code, msg}) when is_atom(code) and is_binary(msg),
    do: {:error, SimpleGrpc.Status.code(code), msg}

  defp normalize_result({:error, code}) when is_atom(code) and code != :error,
    do: {:error, SimpleGrpc.Status.code(code), to_string(code)}

  defp normalize_result({:error, msg}) when is_binary(msg),
    do: {:error, 2, msg}

  defp normalize_result({:error, reason}),
    do: {:error, 2, inspect(reason)}

  defp normalize_result(result),
    do: {:ok, result}
end
