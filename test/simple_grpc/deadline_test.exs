defmodule SimpleGrpc.DeadlineTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Client.Headers
  alias SimpleGrpc.Client

  defmodule SlowServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/streaming/streaming_service.proto"

    def unary_call(req, _stream) do
      # If req.sequence contains sleep duration in milliseconds
      sleep_ms = req.sequence
      if sleep_ms > 0, do: Process.sleep(sleep_ms)

      {:ok, %{result: "Done after #{sleep_ms}ms", sequence: sleep_ms, is_final: true}}
    end

    def server_streaming_call(req, _stream) do
      [req]
    end

    def client_streaming_call(req_stream, _stream) do
      count = Enum.count(req_stream)
      {:ok, %{result: "Total: #{count}", sequence: count, is_final: true}}
    end

    def bidirectional_streaming_call(req_stream, _stream) do
      Stream.map(req_stream, fn req -> req end)
    end
  end

  defmodule SlowClient do
    use SimpleGrpc.Client,
      client_file: "test/fixtures/protos/streaming/streaming_service.proto"
  end

  setup_all do
    {:ok, sup_pid} =
      Bandit.start_link(
        plug: {SimpleGrpc.Plug, services: [SlowServer]},
        port: 0,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup_pid)

    on_exit(fn ->
      if Process.alive?(sup_pid), do: Process.exit(sup_pid, :shutdown)
    end)

    %{port: port}
  end

  describe "Headers.parse_grpc_timeout/1" do
    test "parses hours (H)" do
      assert Headers.parse_grpc_timeout("2H") == {:ok, 7_200_000}
    end

    test "parses minutes (M)" do
      assert Headers.parse_grpc_timeout("5M") == {:ok, 300_000}
    end

    test "parses seconds (S)" do
      assert Headers.parse_grpc_timeout("10S") == {:ok, 10_000}
    end

    test "parses milliseconds (m)" do
      assert Headers.parse_grpc_timeout("500m") == {:ok, 500}
    end

    test "parses microseconds (u)" do
      assert Headers.parse_grpc_timeout("500000u") == {:ok, 500}
    end

    test "parses nanoseconds (n)" do
      assert Headers.parse_grpc_timeout("1000000000n") == {:ok, 1000}
    end

    test "returns :error on invalid string" do
      assert Headers.parse_grpc_timeout("invalid") == :error
      assert Headers.parse_grpc_timeout(nil) == :error
    end
  end

  describe "Server Deadline Auto-Enforcement over HTTP/2 (Bandit)" do
    test "aborts request and returns status 4 when handler exceeds deadline", %{port: port} do
      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      # Request asks server to sleep for 300ms, but sets timeout to 50ms (50m)
      assert {:error, 4, msg} =
               SlowClient.unary_call(
                 conn,
                 %{message: "Sleep300", sequence: 300},
                 headers: [{"grpc-timeout", "50m"}]
               )

      assert msg =~ "deadline exceeded"
      Client.close(conn)
    end

    test "succeeds when handler completes within deadline", %{port: port} do
      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      # Request sleeps for 20ms, deadline is 500ms
      assert {:ok, resp} =
               SlowClient.unary_call(
                 conn,
                 %{message: "Quick", sequence: 20},
                 headers: [{"grpc-timeout", "500m"}]
               )

      assert resp.result == "Done after 20ms"
      Client.close(conn)
    end

    test "aborts immediately when deadline is 0m", %{port: port} do
      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      assert {:error, 4, msg} =
               SlowClient.unary_call(
                 conn,
                 %{message: "Zero", sequence: 100},
                 headers: [{"grpc-timeout", "0m"}]
               )

      assert msg =~ "deadline exceeded"
      Client.close(conn)
    end
  end

  describe "Server Deadline Auto-Enforcement (In-Memory Plug)" do
    test "aborts request on in-memory Plug adapter when handler exceeds deadline" do
      {:ok, conn} = Client.connect({SimpleGrpc.Plug, services: [SlowServer]}, adapter: :plug)

      assert {:error, 4, msg} =
               SlowClient.unary_call(
                 conn,
                 %{message: "SleepPlug", sequence: 250},
                 headers: [{"grpc-timeout", "50m"}]
               )

      assert msg =~ "deadline exceeded"
      Client.close(conn)
    end

    test "succeeds on in-memory Plug adapter when completed within deadline" do
      {:ok, conn} = Client.connect({SimpleGrpc.Plug, services: [SlowServer]}, adapter: :plug)

      assert {:ok, resp} =
               SlowClient.unary_call(
                 conn,
                 %{message: "QuickPlug", sequence: 10},
                 headers: [{"grpc-timeout", "500m"}]
               )

      assert resp.result == "Done after 10ms"
      Client.close(conn)
    end

    test "supports client :deadline option using DateTime", %{port: port} do
      {:ok, conn} = Client.connect("127.0.0.1:#{port}", adapter: :mint)

      # 100ms in the future
      future_deadline = DateTime.add(DateTime.utc_now(), 100, :millisecond)

      # Request sleeps for 300ms, should exceed deadline
      assert {:error, 4, msg} =
               SlowClient.unary_call(
                 conn,
                 %{message: "DeadlineDT", sequence: 300},
                 deadline: future_deadline
               )

      assert msg =~ "deadline exceeded"
      Client.close(conn)
    end

    test "SimpleGrpc.deadline/1 and remaining_timeout/2 helpers extract deadline from conn" do
      plug_conn =
        Plug.Test.conn(:post, "/test")
        |> Plug.Conn.put_req_header("grpc-timeout", "2000m")

      deadline_dt = SimpleGrpc.deadline(plug_conn)
      assert %DateTime{} = deadline_dt
      assert DateTime.diff(deadline_dt, DateTime.utc_now(), :millisecond) in 1500..2500

      assert SimpleGrpc.remaining_timeout(plug_conn) == 2000
      assert SimpleGrpc.remaining_timeout(plug_conn, :second) == 2
      assert SimpleGrpc.remaining_timeout(plug_conn, :microsecond) == 2_000_000
    end
  end
end
