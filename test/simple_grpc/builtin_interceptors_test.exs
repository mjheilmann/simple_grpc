defmodule SimpleGrpc.BuiltinInterceptorsTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Client
  alias SimpleGrpc.Context
  alias SimpleGrpc.TraceContext

  defmodule TraceRecordingServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/custom_prefix/custom_prefix_service.proto",
      interceptors: [SimpleGrpc.Interceptors.TraceContext]

    def echo(%{message: msg}, ctx) do
      trace_id = Context.get_private(ctx, :trace_id)
      server_tc = Context.get_private(ctx, :trace_context)

      {:ok,
       %{
         reply:
           "Hello #{msg} (trace_id=#{trace_id}, parent_id=#{server_tc && server_tc.parent_id})"
       }}
    end
  end

  defmodule AuthSecuredServer do
    use SimpleGrpc.Server,
      server_file: "test/fixtures/protos/custom_prefix/custom_prefix_service.proto",
      interceptors: [
        {SimpleGrpc.Interceptors.Auth,
         validator: fn token, _ctx ->
           if token == "Bearer valid-super-token" do
             {:ok, %{username: "SuperAdmin"}}
           else
             {:error, "invalid credentials"}
           end
         end}
      ]

    def echo(%{message: msg}, ctx) do
      user = Context.get_private(ctx, :auth_user)
      {:ok, %{reply: "Welcome #{user.username}, greeting for #{msg}"}}
    end
  end

  defmodule EchoClient do
    use SimpleGrpc.Client,
      client_file: "test/fixtures/protos/custom_prefix/custom_prefix_service.proto"
  end

  describe "OpenTelemetry / TraceContext interceptor propagation" do
    test "propagates traceparent from client to server over in-memory plug" do
      plug_state = SimpleGrpc.Plug.init(services: [TraceRecordingServer])

      # Trace interceptor on client
      client_trace_tc = TraceContext.generate()

      client_trace_interceptor = fn req, ctx, next ->
        ctx = TraceContext.inject(ctx, client_trace_tc)
        next.(req, ctx)
      end

      {:ok, conn} =
        Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          plug_opts: plug_state,
          interceptors: [client_trace_interceptor]
        )

      assert {:ok, resp} = EchoClient.echo(conn, %{message: "Alice"})

      assert resp.reply =~ "trace_id=#{client_trace_tc.trace_id}"
      assert resp.reply =~ "parent_id=#{client_trace_tc.span_id}"
    end

    test "builtin OpenTelemetry interceptor automatically generates and propagates traceparent" do
      plug_state = SimpleGrpc.Plug.init(services: [TraceRecordingServer])

      {:ok, conn} =
        Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          plug_opts: plug_state,
          interceptors: [SimpleGrpc.Interceptors.OpenTelemetry]
        )

      assert {:ok, resp} = EchoClient.echo(conn, %{message: "Bob"})
      assert resp.reply =~ ~r/trace_id=[0-9a-f]{32}/
      assert resp.reply =~ ~r/parent_id=[0-9a-f]{16}/
    end
  end

  describe "Auth interceptor" do
    test "client injects token and server validates successfully" do
      plug_state = SimpleGrpc.Plug.init(services: [AuthSecuredServer])

      {:ok, conn} =
        Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          plug_opts: plug_state,
          interceptors: [{SimpleGrpc.Interceptors.Auth, bearer: "valid-super-token"}]
        )

      assert {:ok, resp} = EchoClient.echo(conn, %{message: "Charlie"})
      assert resp.reply == "Welcome SuperAdmin, greeting for Charlie"
    end

    test "client with invalid token is rejected by server Auth interceptor" do
      plug_state = SimpleGrpc.Plug.init(services: [AuthSecuredServer])

      {:ok, conn} =
        Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          plug_opts: plug_state,
          interceptors: [{SimpleGrpc.Interceptors.Auth, bearer: "wrong-token"}]
        )

      assert {:error, 16, msg} = EchoClient.echo(conn, %{message: "Charlie"})
      assert msg =~ "Unauthenticated"
    end
  end

  describe "Logger interceptor" do
    test "executes cleanly without modifying result or crashing" do
      plug_state =
        SimpleGrpc.Plug.init(
          services: [TraceRecordingServer],
          interceptors: [{SimpleGrpc.Interceptors.Logger, level: :debug}]
        )

      {:ok, conn} =
        Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          plug_opts: plug_state,
          interceptors: [{SimpleGrpc.Interceptors.Logger, level: :debug}]
        )

      assert {:ok, resp} = EchoClient.echo(conn, %{message: "LoggedUser"})
      assert resp.reply =~ "Hello LoggedUser"
    end
  end

  describe "HTTP/2 Network transport with Interceptors" do
    test "propagates traceparent over actual HTTP/2 network socket" do
      server =
        start_supervised!(
          {Bandit,
           plug:
             {SimpleGrpc.Plug,
              services: [TraceRecordingServer],
              interceptors: [SimpleGrpc.Interceptors.TraceContext]},
           scheme: :http,
           port: 0,
           startup_log: false}
        )

      {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

      {:ok, conn} =
        Client.connect("http://localhost:#{port}",
          adapter: :mint,
          interceptors: [SimpleGrpc.Interceptors.OpenTelemetry]
        )

      assert {:ok, resp} = EchoClient.echo(conn, %{message: "NetworkTrace"})
      assert resp.reply =~ ~r/trace_id=[0-9a-f]{32}/
      assert resp.reply =~ ~r/parent_id=[0-9a-f]{16}/

      Client.close(conn)
    end
  end
end
