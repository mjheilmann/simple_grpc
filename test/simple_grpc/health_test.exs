defmodule SimpleGrpc.HealthTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.Health
  alias SimpleGrpc.Client

  defmodule HealthClient do
    use SimpleGrpc.Client,
      client_file: "priv/protos/grpc/health/v1/health.proto"
  end

  setup do
    Health.clear_all()
    :ok
  end

  setup_all do
    {:ok, sup_pid} =
      Bandit.start_link(
        plug: {SimpleGrpc.Plug, health: true},
        port: 0,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup_pid)

    on_exit(fn ->
      if Process.alive?(sup_pid), do: Process.exit(sup_pid, :shutdown)
    end)

    %{port: port}
  end

  describe "Health Status Management" do
    test "defaults overall status to :SERVING" do
      assert Health.get_status("") == :SERVING
    end

    test "sets and gets service statuses" do
      Health.set_status("service.A", :SERVING)
      Health.set_status("service.B", :NOT_SERVING)
      # :SERVICE_UNKNOWN
      Health.set_status("service.C", 3)

      assert Health.get_status("service.A") == :SERVING
      assert Health.get_status("service.B") == :NOT_SERVING
      assert Health.get_status("service.C") == :SERVICE_UNKNOWN

      Health.set_not_serving("service.A")
      assert Health.get_status("service.A") == :NOT_SERVING

      Health.set_serving("service.A")
      assert Health.get_status("service.A") == :SERVING

      Health.clear_status("service.A")
      assert Health.get_status("service.A") == nil
    end

    test "subscribes and receives notifications on status change" do
      Health.subscribe("payment.Service")

      Health.set_status("payment.Service", :NOT_SERVING)
      assert_receive {:"$health_status_changed", "payment.Service", :NOT_SERVING}, 500

      Health.set_serving("payment.Service")
      assert_receive {:"$health_status_changed", "payment.Service", :SERVING}, 500

      Health.unsubscribe("payment.Service")
      Health.set_not_serving("payment.Service")
      refute_receive {:"$health_status_changed", "payment.Service", _}, 100
    end
  end

  describe "Health Check RPC over HTTP/2 (Bandit)" do
    test "checks overall server status", %{port: port} do
      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      assert {:ok, resp} = HealthClient.check(conn, %{service: ""})
      assert resp.status == :SERVING
      Client.close(conn)
    end

    test "checks specific registered service status", %{port: port} do
      Health.set_status("auth.AuthService", :SERVING)
      Health.set_status("billing.BillingService", :NOT_SERVING)

      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      assert {:ok, resp1} = HealthClient.check(conn, %{service: "auth.AuthService"})
      assert resp1.status == :SERVING

      assert {:ok, resp2} = HealthClient.check(conn, %{service: "billing.BillingService"})
      assert resp2.status == :NOT_SERVING

      Client.close(conn)
    end

    test "returns NOT_FOUND (5) when service is unregistered", %{port: port} do
      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      assert {:error, 5, msg} = HealthClient.check(conn, %{service: "nonexistent.Service"})
      assert msg =~ "not found"
      Client.close(conn)
    end
  end

  describe "Health Watch Streaming RPC" do
    test "streams initial status and subsequent status changes over HTTP/2", %{port: port} do
      Health.set_status("stream.Service", :SERVING)

      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      assert {:ok, stream} = HealthClient.watch(conn, %{service: "stream.Service"})

      # Spawn background updater
      Task.start(fn ->
        Process.sleep(100)
        Health.set_not_serving("stream.Service")
        Process.sleep(100)
        Health.set_serving("stream.Service")
      end)

      # Take 3 messages from the stream
      events =
        stream
        |> Stream.take(3)
        |> Enum.to_list()

      assert length(events) == 3
      assert Enum.at(events, 0).status == :SERVING
      assert Enum.at(events, 1).status == :NOT_SERVING
      assert Enum.at(events, 2).status == :SERVING

      Client.close(conn)
    end
  end

  describe "In-Memory Plug Transport Health Check" do
    test "checks health via Plug adapter" do
      Health.set_status("plug.Service", :SERVING)
      {:ok, conn} = Client.connect({SimpleGrpc.Plug, health: true}, adapter: :plug)

      assert {:ok, resp} = HealthClient.check(conn, %{service: "plug.Service"})
      assert resp.status == :SERVING
      Client.close(conn)
    end
  end
end
