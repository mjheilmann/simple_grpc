defmodule SimpleGrpc.Pipeline do
  @moduledoc """
  Composes and executes client and server interceptor pipelines.
  """

  alias SimpleGrpc.Context

  @doc """
  Executes an ordered pipeline of interceptors ending with `final_fn`.

  Interceptors are executed in order (the first interceptor is called first and
  returns last). Each interceptor receives `(req, ctx, next)` where `next` is
  `(req, ctx) -> result`.

  ## Supported Interceptors
  - Anonymous functions: `fn req, ctx, next -> next.(req, ctx) end`
  - Modules: `MyInterceptor` (invokes `MyInterceptor.intercept(req, ctx, next)`)
  - Tuples: `{MyInterceptor, opts}` (invokes `MyInterceptor.intercept(req, ctx, next, opts)`)
  """
  def execute(interceptors, req, %Context{} = ctx, final_fn) when is_list(interceptors) and is_function(final_fn, 2) do
    flattened = List.flatten(interceptors) |> Enum.reject(&is_nil/1)

    chain =
      Enum.reduce(Enum.reverse(flattened), final_fn, fn interceptor, next_fn ->
        build_step(interceptor, next_fn)
      end)

    chain.(req, ctx)
  end

  def execute(nil, req, %Context{} = ctx, final_fn) when is_function(final_fn, 2) do
    final_fn.(req, ctx)
  end

  def execute(single_interceptor, req, %Context{} = ctx, final_fn) when is_function(final_fn, 2) do
    execute([single_interceptor], req, ctx, final_fn)
  end

  defp build_step(fun, next_fn) when is_function(fun, 3) do
    fn req, ctx ->
      fun.(req, ctx, next_fn)
    end
  end

  defp build_step({fun, opts}, next_fn) when is_function(fun, 4) do
    fn req, ctx ->
      fun.(req, ctx, next_fn, opts)
    end
  end

  defp build_step({mod, opts}, next_fn) when is_atom(mod) do
    Code.ensure_loaded(mod)

    cond do
      function_exported?(mod, :intercept, 4) ->
        fn req, ctx ->
          mod.intercept(req, ctx, next_fn, opts)
        end

      function_exported?(mod, :intercept, 3) ->
        fn req, ctx ->
          mod.intercept(req, ctx, next_fn)
        end

      true ->
        raise ArgumentError, "Module #{inspect(mod)} must implement SimpleGrpc.Interceptor.intercept/3 or /4"
    end
  end

  defp build_step(mod, next_fn) when is_atom(mod) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :intercept, 3) do
      fn req, ctx ->
        mod.intercept(req, ctx, next_fn)
      end
    else
      raise ArgumentError, "Module #{inspect(mod)} must implement SimpleGrpc.Interceptor.intercept/3"
    end
  end

  defp build_step(other, _next_fn) do
    raise ArgumentError, "Invalid interceptor #{inspect(other)}. Expected function or module."
  end
end
