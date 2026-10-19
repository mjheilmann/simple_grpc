defmodule SimpleGrpc.PlugServiceTest do
  use SimpleGrpc.Case, plug: __MODULE__.Pipeline

  import Plug.Test

  alias SimpleGrpc.Protobuf

  @files ["test/fixtures/protos/two_services/two_services.proto"]

  defmodule AlphaServer do
    use SimpleGrpc.Server,
      files: ["test/fixtures/protos/two_services/two_services.proto"],
      service: "Alpha"

    def echo(req, _ctx), do: {:ok, %{text: "alpha:" <> req.text, level: req.level}}
    def only_alpha(req, _ctx), do: {:ok, %{text: "only alpha:" <> req.text}}
    def count(req, _ctx), do: Stream.map(1..3, &%{text: "#{req.text}#{&1}"})
    def boom(_req, _ctx), do: raise("boom")
  end

  defmodule BetaServer do
    use SimpleGrpc.Server,
      files: ["test/fixtures/protos/two_services/two_services.proto"],
      service: "Beta"

    # arity-1 handlers are accepted too
    def echo(req), do: {:ok, %{text: "beta:" <> req.text, level: req.level}}
    def only_beta(req, _ctx), do: {:ok, %{text: "only beta:" <> req.text}}
  end

  defmodule HandlerlessService do
    @behaviour SimpleGrpc.Service

    @impl true
    def __schema__, do: SimpleGrpc.PlugServiceTest.AlphaServer.__schema__()
    @impl true
    def __service_name__, do: "Alpha"
    @impl true
    def __grpc_service_path__, do: "/twosvc.Alpha"
    @impl true
    def __rpcs__, do: []
    @impl true
    def __interceptors__, do: []
  end

  defmodule Pipeline do
    use Plug.Builder

    plug(SimpleGrpc.Plug.Service, server: SimpleGrpc.PlugServiceTest.AlphaServer)
    plug(SimpleGrpc.Plug.Service, server: SimpleGrpc.PlugServiceTest.BetaServer)
    plug(SimpleGrpc.Plug.NotFound)
  end

  defmodule ReversePipeline do
    use Plug.Builder

    plug(SimpleGrpc.Plug.Service, server: SimpleGrpc.PlugServiceTest.BetaServer)
    plug(SimpleGrpc.Plug.Service, server: SimpleGrpc.PlugServiceTest.AlphaServer)
    plug(SimpleGrpc.Plug.NotFound)
  end

  defp encode(attrs), do: Protobuf.encode!("twosvc.Msg", attrs, AlphaServer.__schema__())

  defp decode(bytes), do: Protobuf.decode!("twosvc.Msg", bytes, AlphaServer.__schema__())

  defp call(conn, path, attrs) do
    case grpc_call(conn, path, encode(attrs)) do
      {:ok, resp, _headers, _trailers} -> {:ok, decode(resp)}
      other -> other
    end
  end

  describe "two services in one pipeline" do
    test "a same-named RPC reaches its own service", %{conn: conn} do
      assert {:ok, %{text: "alpha:hi", level: :HIGH}} =
               call(conn, "/twosvc.Alpha/Echo", %{text: "hi", level: :HIGH})

      assert {:ok, %{text: "beta:hi", level: :HIGH}} =
               call(conn, "/twosvc.Beta/Echo", %{text: "hi", level: :HIGH})
    end

    test "each service serves its own other RPCs", %{conn: conn} do
      assert {:ok, %{text: "only alpha:x"}} = call(conn, "/twosvc.Alpha/OnlyAlpha", %{text: "x"})
      assert {:ok, %{text: "only beta:x"}} = call(conn, "/twosvc.Beta/OnlyBeta", %{text: "x"})
    end

    @tag plug: __MODULE__.ReversePipeline
    test "plug order does not change who answers", %{conn: conn} do
      assert {:ok, %{text: "alpha:hi"}} = call(conn, "/twosvc.Alpha/Echo", %{text: "hi"})
      assert {:ok, %{text: "beta:hi"}} = call(conn, "/twosvc.Beta/Echo", %{text: "hi"})
    end

    test "a service does not answer another service's RPC", %{conn: conn} do
      assert {:error, 12, _} = call(conn, "/twosvc.Alpha/OnlyBeta", %{text: "x"})
      assert {:error, 12, _} = call(conn, "/twosvc.Beta/OnlyAlpha", %{text: "x"})
    end

    test "server streaming works through the plug", %{conn: conn} do
      {:ok, frames, _headers, _trailers} =
        grpc_stream(conn, "/twosvc.Alpha/Count", encode(%{text: "n"}))

      assert Enum.map(frames, &decode(&1).text) == ["n1", "n2", "n3"]
    end
  end

  describe "unknown and unimplemented" do
    test "an unknown method under a claimed prefix is UNIMPLEMENTED", %{conn: conn} do
      assert {:error, 12, message} = call(conn, "/twosvc.Alpha/Nope", %{text: "x"})
      assert message =~ "/twosvc.Alpha/Nope"
    end

    test "a service module without a handler function answers UNIMPLEMENTED" do
      # `use SimpleGrpc.Server` refuses to compile without handlers; a hand-written
      # implementation of the behaviour can still lack one.
      {:ok, conn} =
        SimpleGrpc.Client.connect(SimpleGrpc.Plug.Service,
          adapter: :plug,
          plug_opts: SimpleGrpc.Plug.Service.init(server: HandlerlessService)
        )

      assert {:error, 12, message} = call(conn, "/twosvc.Alpha/Echo", %{text: "x"})
      assert message =~ "No handler found for Echo"
    end

    test "an unknown service reaches NotFound, which answers UNIMPLEMENTED", %{conn: conn} do
      assert {:error, 12, _} = call(conn, "/other.Service/Echo", %{text: "x"})
    end

    test "a crashing handler is INTERNAL", %{conn: conn} do
      assert {:error, 13, _} = call(conn, "/twosvc.Alpha/Boom", %{text: "x"})
    end
  end

  describe "claiming requests" do
    setup do
      %{state: SimpleGrpc.Plug.Service.init(server: AlphaServer)}
    end

    test "a request for another service passes through untouched", %{state: state} do
      conn = conn(:post, "/twosvc.Beta/Echo", "")
      result = SimpleGrpc.Plug.Service.call(conn, state)

      refute result.halted
      assert result.state == :unset
    end

    test "a path sharing only the service name prefix is not claimed", %{state: state} do
      conn = conn(:post, "/twosvc.AlphaExtra/Echo", "")
      refute SimpleGrpc.Plug.Service.call(conn, state).halted
    end

    test "a request it handles is halted", %{state: state} do
      conn =
        conn(:post, "/twosvc.Alpha/Nope", "")
        |> Plug.Conn.put_req_header("content-type", "application/grpc")

      assert SimpleGrpc.Plug.Service.call(conn, state).halted
    end
  end

  describe "init" do
    test "requires a server" do
      assert_raise ArgumentError, ~r/requires a :server/, fn ->
        SimpleGrpc.Plug.Service.init([])
      end
    end

    test "rejects a module that is not a service" do
      assert_raise ArgumentError, ~r/is not a SimpleGrpc.Service/, fn ->
        SimpleGrpc.Plug.Service.init(server: Enum)
      end

      assert_raise ArgumentError, ~r/expected a module/, fn ->
        SimpleGrpc.Plug.Service.init(server: "nope")
      end
    end

    test "builds routes for its own service only" do
      routes = SimpleGrpc.Service.cached_routes(AlphaServer)

      assert routes |> Map.keys() |> Enum.sort() ==
               Enum.sort([
                 "/twosvc.Alpha/Echo",
                 "/twosvc.Alpha/OnlyAlpha",
                 "/twosvc.Alpha/Count",
                 "/twosvc.Alpha/Boom"
               ])
    end
  end

  defmodule NotifyInterceptor do
    @behaviour SimpleGrpc.Interceptor

    @impl true
    def intercept(req, ctx, next, opts) do
      send(Process.whereis(:plug_service_test), {:intercepted, opts[:tag], ctx.rpc_name})
      next.(req, ctx)
    end
  end

  defmodule InterceptedPipeline do
    use Plug.Builder

    plug(SimpleGrpc.Plug.Service,
      server: SimpleGrpc.PlugServiceTest.AlphaServer,
      interceptors: [{SimpleGrpc.PlugServiceTest.NotifyInterceptor, tag: :alpha}]
    )

    plug(SimpleGrpc.Plug.Service, server: SimpleGrpc.PlugServiceTest.BetaServer)
    plug(SimpleGrpc.Plug.NotFound)
  end

  describe "interceptors" do
    @tag plug: __MODULE__.InterceptedPipeline
    test "plug-level interceptors run for that service only", %{conn: conn} do
      Process.register(self(), :plug_service_test)

      assert {:ok, _} = call(conn, "/twosvc.Alpha/Echo", %{text: "a"})
      assert_received {:intercepted, :alpha, "Echo"}

      assert {:ok, _} = call(conn, "/twosvc.Beta/Echo", %{text: "b"})
      refute_received {:intercepted, _, _}
    end
  end

  describe "telemetry" do
    test "events carry the serving module", %{conn: conn} do
      ref = make_ref()
      test_pid = self()

      :telemetry.attach_many(
        "plug-service-#{inspect(ref)}",
        [[:simple_grpc, :call, :start], [:simple_grpc, :call, :stop]],
        fn event, _m, meta, _ -> send(test_pid, {ref, event, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("plug-service-#{inspect(ref)}") end)

      assert {:ok, _} = call(conn, "/twosvc.Beta/Echo", %{text: "t"})

      assert_receive {^ref, [:simple_grpc, :call, :start], %{server: BetaServer, rpc: "Echo"}}
      assert_receive {^ref, [:simple_grpc, :call, :stop], %{server: BetaServer, grpc_status: 0}}
    end
  end

  describe "NotFound" do
    test "answers plain HTTP with 404, or 415 for a POST to a gRPC-shaped path" do
      state = SimpleGrpc.Plug.NotFound.init([])

      assert %{status: 404, halted: true} =
               SimpleGrpc.Plug.NotFound.call(conn(:get, "/index.html"), state)

      assert %{status: 415} =
               SimpleGrpc.Plug.NotFound.call(
                 conn(:post, "/some.Service/Method", "")
                 |> Plug.Conn.put_req_header("content-type", "application/json"),
                 state
               )
    end
  end

  test "files used by the tests exist" do
    assert Enum.all?(@files, &File.exists?/1)
  end
end
