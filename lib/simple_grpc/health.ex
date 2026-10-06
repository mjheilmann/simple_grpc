defmodule SimpleGrpc.Health do
  @moduledoc """
  Implements the canonical gRPC Health Checking Protocol (`grpc.health.v1`).
  Provides `Check` and `Watch` RPCs for Kubernetes liveness/readiness probes and load balancers.
  """

  use GenServer

  alias SimpleGrpc.{Registry, Schema}

  @doc """
  Registers the Health service in `SimpleGrpc.Registry`.

  ## Options
  - `:statuses` - map or keyword list of initial `%{service_name => status}` mappings.
  """
  def register(opts \\ []) do
    apply_initial_statuses(opts)
    schema = schema()
    Registry.register_service(schema, __MODULE__)
    :ok
  end

  @doc """
  Builds the route map for the Health service.

  ## Options
  - `:statuses` - map or keyword list of initial `%{service_name => status}` mappings.
  """
  def build_health_routes(opts \\ []) do
    apply_initial_statuses(opts)
    schema = schema()
    {:ok, routes} = Registry.generate_routes(schema, __MODULE__)
    routes
  end

  @table_name :simple_grpc_health_status_table
  @subscribers_table :simple_grpc_health_subscribers_table

  @type status :: :SERVING | :NOT_SERVING | :UNKNOWN | :SERVICE_UNKNOWN

  # ===========================================================================
  # Service Behaviour & Schema
  # ===========================================================================

  @doc """
  Returns the `%SimpleGrpc.Schema{}` struct for the `grpc.health.v1.Health` service.
  """
  def __schema__ do
    schema()
  end

  @doc """
  Returns the parsed schema struct for `grpc.health.v1`.
  """
  def schema do
    proto_path = Path.join(:code.priv_dir(:simple_grpc), "protos/grpc/health/v1/health.proto")

    case Schema.from_file(proto_path) do
      {:ok, s} ->
        s

      {:error, _} ->
        # Fallback to local priv path if priv_dir is not yet loaded in test
        {:ok, s} = Schema.from_file("priv/protos/grpc/health/v1/health.proto")
        s
    end
  end

  # ===========================================================================
  # Lifecycle & GenServer
  # ===========================================================================

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    ensure_tables_exist()
    {:ok, %{}}
  end

  # ===========================================================================
  # Public Health Status Management API
  # ===========================================================================

  @doc """
  Sets the health status of a service to `:SERVING` (status code 1).
  Defaults to `""` for the overall server status.
  """
  def set_serving(service \\ "") do
    set_status(service, :SERVING)
  end

  @doc """
  Sets the health status of a service to `:NOT_SERVING` (status code 2).
  Defaults to `""` for the overall server status.
  """
  def set_not_serving(service \\ "") do
    set_status(service, :NOT_SERVING)
  end

  @doc """
  Sets the health status of a service to the given status.
  Status can be `:SERVING`, `:NOT_SERVING`, `:UNKNOWN`, `:SERVICE_UNKNOWN`, or integer `0..3`.
  """
  def set_status(service \\ "", status) do
    ensure_tables_exist()
    norm_status = normalize_status(status)
    service_str = to_string(service)

    :ets.insert(@table_name, {service_str, norm_status})
    broadcast_status(service_str, norm_status)
    :ok
  end

  @doc """
  Returns the current health status of a service, or `nil` if not registered.
  """
  def get_status(service \\ "") do
    ensure_tables_exist()
    service_str = to_string(service)

    case :ets.lookup(@table_name, service_str) do
      [{^service_str, status}] -> status
      [] -> nil
    end
  end

  @doc """
  Removes the registered health status of a service.
  """
  def clear_status(service \\ "") do
    ensure_tables_exist()
    service_str = to_string(service)
    :ets.delete(@table_name, service_str)
    broadcast_status(service_str, :SERVICE_UNKNOWN)
    :ok
  end

  @doc """
  Clears all registered service health statuses and resets overall server status to `:SERVING`.
  """
  def clear_all do
    ensure_tables_exist()
    :ets.delete_all_objects(@table_name)
    :ets.insert(@table_name, {"", :SERVING})
    :ok
  end

  @doc """
  Subscribes the caller process to health status changes for `service`.
  """
  def subscribe(service \\ "") do
    ensure_tables_exist()
    service_str = to_string(service)
    pid = self()
    :ets.insert(@subscribers_table, {service_str, pid})
    :ok
  end

  @doc """
  Unsubscribes the caller process from health status changes for `service`.
  """
  def unsubscribe(service \\ "") do
    ensure_tables_exist()
    service_str = to_string(service)
    pid = self()
    :ets.match_delete(@subscribers_table, {service_str, pid})
    :ok
  end

  # ===========================================================================
  # gRPC Handlers: Check & Watch
  # ===========================================================================

  @doc """
  Handles unary `Check` RPC calls.
  """
  def check(req) do
    service = (is_map(req) && Map.get(req, :service)) || ""

    case get_status(service) do
      nil ->
        if service == "" do
          {:ok, %{status: :SERVING}}
        else
          {:error, 5, "service #{inspect(service)} not found"}
        end

      status ->
        {:ok, %{status: status}}
    end
  end

  def check(req, _stream_or_conn), do: check(req)

  @doc """
  Handles server-streaming `Watch` RPC calls.
  """
  def watch(req) do
    service = (is_map(req) && Map.get(req, :service)) || ""
    initial_status = get_status(service) || if service == "", do: :SERVING, else: :SERVICE_UNKNOWN

    Stream.resource(
      fn ->
        subscribe(service)
        {:initial, initial_status}
      end,
      fn
        {:initial, status} ->
          {[%{status: status}], :subscribed}

        :subscribed ->
          receive do
            {:"$health_status_changed", ^service, new_status} ->
              {[%{status: new_status}], :subscribed}

            {:"$health_status_changed", "", new_status} when service == "" ->
              {[%{status: new_status}], :subscribed}
          after
            5000 ->
              {[], :subscribed}
          end

        :halt ->
          {:halt, :halt}
      end,
      fn _state ->
        unsubscribe(service)
      end
    )
  end

  def watch(req, _stream_or_conn), do: watch(req)

  # ===========================================================================
  # Internal Helpers
  # ===========================================================================

  defp ensure_tables_exist do
    if :ets.info(@table_name) == :undefined do
      try do
        :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
        :ets.insert(@table_name, {"", :SERVING})
      catch
        _, _ -> :ok
      end
    end

    if :ets.info(@subscribers_table) == :undefined do
      try do
        :ets.new(@subscribers_table, [:bag, :public, :named_table, read_concurrency: true])
      catch
        _, _ -> :ok
      end
    end

    :ok
  end

  defp broadcast_status(service, status) do
    ensure_tables_exist()

    subscribers =
      try do
        :ets.lookup(@subscribers_table, service)
      catch
        _, _ -> []
      end

    Enum.each(subscribers, fn {_svc, pid} ->
      if Process.alive?(pid) do
        send(pid, {:"$health_status_changed", service, status})
      end
    end)
  end

  defp normalize_status(1), do: :SERVING
  defp normalize_status(2), do: :NOT_SERVING
  defp normalize_status(3), do: :SERVICE_UNKNOWN
  defp normalize_status(0), do: :UNKNOWN
  defp normalize_status(:SERVING), do: :SERVING
  defp normalize_status(:NOT_SERVING), do: :NOT_SERVING
  defp normalize_status(:SERVICE_UNKNOWN), do: :SERVICE_UNKNOWN
  defp normalize_status(:UNKNOWN), do: :UNKNOWN
  defp normalize_status("SERVING"), do: :SERVING
  defp normalize_status("NOT_SERVING"), do: :NOT_SERVING
  defp normalize_status("SERVICE_UNKNOWN"), do: :SERVICE_UNKNOWN
  defp normalize_status("UNKNOWN"), do: :UNKNOWN
  defp normalize_status(_), do: :UNKNOWN

  defp apply_initial_statuses(opts) when is_list(opts) do
    case Keyword.get(opts, :statuses) do
      statuses when is_map(statuses) or is_list(statuses) ->
        Enum.each(statuses, fn {svc, status} -> set_status(svc, status) end)

      _ ->
        :ok
    end
  end

  defp apply_initial_statuses(_), do: :ok
end
