defmodule SimpleGrpc.Interceptor do
  @moduledoc """
  Behaviour for client-side and server-side gRPC interceptors / middleware.

  Interceptors follow a composable pipeline pattern:
  `(req, ctx, next) -> next.(req, ctx)`

  ## Module-based Interceptor Example
      defmodule MyApp.LoggingInterceptor do
        @behaviour SimpleGrpc.Interceptor

        @impl true
        def intercept(req, ctx, next) do
          start_time = System.monotonic_time()
          result = next.(req, ctx)
          duration = System.monotonic_time() - start_time
          Logger.info("RPC \#{ctx.path} finished in \#{duration}ns")
          result
        end
      end

  ## Function-based Interceptor Example
      auth_interceptor = fn req, ctx, next ->
        if ctx.role == :client do
          ctx = SimpleGrpc.Context.put_header(ctx, "authorization", "Bearer my-token")
          next.(req, ctx)
        else
          next.(req, ctx)
        end
      end
  """

  @type req :: any()
  @type ctx :: SimpleGrpc.Context.t()
  @type next_fn :: (req, ctx -> {:ok, any()} | {:error, any()} | any())
  @type result :: {:ok, any()} | {:error, any()} | any()

  @callback intercept(req :: req(), ctx :: ctx(), next :: next_fn()) :: result()
  @callback intercept(req :: req(), ctx :: ctx(), next :: next_fn(), opts :: any()) :: result()

  @optional_callbacks [intercept: 3, intercept: 4]
end
