defmodule SimpleGrpc.Client.Pool do
  @moduledoc """
  High-performance connection pool for gRPC HTTP/2 transport connections powered by `NimblePool`.

  Manages a pool of multiplexed `Mint.HTTP2` transport connections, distributing
  concurrent requests across subchannels, automatically replacing failed or terminated connections,
  and providing configurable pool sizing.

  ## Options
  - `:pool_size` - Number of concurrent HTTP/2 connections in the pool (default: `System.schedulers_online()`).
  - `:lazy` - When `true`, connections are established lazily on first request. When `false`, connections are established at startup (default: `false`).
  - `:worker_idle_timeout` - Maximum time in ms a worker can remain idle before being disconnected (default: `:infinity`).
  """

  @behaviour NimblePool

  alias SimpleGrpc.Client.Adapters.Mint.Connection

  @default_checkout_timeout 5_000

  @doc """
  Starts a connection pool linked to the calling process.
  """
  def start_link(target, opts \\ []) do
    pool_size = Keyword.get(opts, :pool_size, default_pool_size())
    lazy = Keyword.get(opts, :lazy, false)
    base_opts = [
      worker: {__MODULE__, {target, opts}},
      pool_size: max(pool_size, 1),
      lazy: lazy
    ]

    nimble_opts =
      case Keyword.get(opts, :worker_idle_timeout) do
        timeout when is_integer(timeout) and timeout > 0 ->
          Keyword.put(base_opts, :worker_idle_timeout, timeout)

        _ ->
          base_opts
      end

    NimblePool.start_link(nimble_opts)
  end

  defp default_pool_size do
    max(System.schedulers_online(), 2)
  end

  # ===========================================================================
  # NimblePool Callbacks
  # ===========================================================================

  @impl NimblePool
  def init_pool(pool_state) do
    {:ok, pool_state}
  end

  @impl NimblePool
  def init_worker({target, opts} = pool_state) do
    case Connection.start_link(target, opts) do
      {:ok, pid} ->
        {:ok, pid, pool_state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, worker_pid, pool_state) do
    if is_pid(worker_pid) and Process.alive?(worker_pid) do
      {:ok, worker_pid, worker_pid, pool_state}
    else
      {:remove, :dead_worker, pool_state}
    end
  end

  @impl NimblePool
  def handle_checkin(:ok, _from, worker_pid, pool_state) do
    if is_pid(worker_pid) and Process.alive?(worker_pid) do
      {:ok, worker_pid, pool_state}
    else
      {:remove, :dead_worker, pool_state}
    end
  end

  @impl NimblePool
  def handle_ping(worker_pid, pool_state) do
    if is_pid(worker_pid) and Process.alive?(worker_pid) do
      {:ok, worker_pid, pool_state}
    else
      {:remove, :dead_worker, pool_state}
    end
  end

  @impl NimblePool
  def terminate_worker(_reason, worker_pid, pool_state) do
    if is_pid(worker_pid) and Process.alive?(worker_pid) do
      try do
        GenServer.stop(worker_pid, :normal, 1000)
      catch
        :exit, _ -> :ok
      end
    end

    {:ok, pool_state}
  end

  # ===========================================================================
  # Client Pool API
  # ===========================================================================

  @doc """
  Executes a function with a checked-out connection worker from the pool.
  """
  def checkout(pool, fun, timeout \\ @default_checkout_timeout)
      when is_pid(pool) and is_function(fun, 1) do
    NimblePool.checkout!(
      pool,
      :checkout,
      fn _from, worker_pid ->
        result = fun.(worker_pid)
        {result, :ok}
      end,
      timeout
    )
  end

  @doc """
  Sends an HTTP/2 PING frame on a checked-out connection worker.
  """
  def ping(pool, opts \\ []) when is_pid(pool) do
    timeout = Keyword.get(opts, :timeout, @default_checkout_timeout)

    checkout(
      pool,
      fn worker_pid ->
        GenServer.call(worker_pid, {:ping, Keyword.get(opts, :payload), timeout}, timeout + 2000)
      end,
      timeout + 2000
    )
  end

  @doc """
  Closes the pool and terminates all worker connections.
  """
  def close(pool) when is_pid(pool) do
    if Process.alive?(pool) do
      try do
        GenServer.stop(pool, :normal, 2000)
      catch
        :exit, _ -> :ok
      end
    else
      :ok
    end
  end

  def close(_), do: :ok
end
