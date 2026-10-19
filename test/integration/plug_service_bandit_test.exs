defmodule SimpleGrpc.PlugServiceBanditTest do
  use ExUnit.Case, async: false

  @proto "test/fixtures/protos/two_services/two_services.proto"

  defmodule Alpha do
    use SimpleGrpc.Server,
      files: ["test/fixtures/protos/two_services/two_services.proto"],
      service: "Alpha"

    def echo(req, _ctx), do: {:ok, %{text: "alpha:" <> req.text}}
    def only_alpha(req, _ctx), do: {:ok, req}
    def count(req, _ctx), do: Stream.map(1..3, &%{text: "#{req.text}#{&1}"})
    def boom(_req, _ctx), do: raise("boom")
  end

  defmodule Beta do
    use SimpleGrpc.Server,
      files: ["test/fixtures/protos/two_services/two_services.proto"],
      service: "Beta"

    def echo(req, _ctx), do: {:ok, %{text: "beta:" <> req.text}}
    def only_beta(req, _ctx), do: {:ok, req}
  end

  defmodule Pipeline do
    use Plug.Builder

    plug(SimpleGrpc.Plug.Service, server: SimpleGrpc.PlugServiceBanditTest.Alpha)
    plug(SimpleGrpc.Plug.Service, server: SimpleGrpc.PlugServiceBanditTest.Beta)
    plug(SimpleGrpc.Plug.NotFound)
  end

  setup_all do
    grpcurl = System.find_executable("grpcurl") || "/opt/homebrew/bin/grpcurl"
    {:ok, sup} = Bandit.start_link(plug: Pipeline, port: 0, startup_log: false)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(sup)
    on_exit(fn -> if Process.alive?(sup), do: Process.exit(sup, :shutdown) end)
    %{grpcurl: grpcurl, port: port}
  end

  defp grpcurl(ctx, method, data) do
    System.cmd(
      ctx.grpcurl,
      ["-plaintext", "-proto", @proto, "-d", data, "127.0.0.1:#{ctx.port}", method],
      stderr_to_stdout: true
    )
  end

  test "both services answer their own same-named RPC over HTTP/2", ctx do
    assert {out, 0} = grpcurl(ctx, "twosvc.Alpha/Echo", ~s({"text": "x"}))
    assert out =~ ~s("text": "alpha:x")

    assert {out, 0} = grpcurl(ctx, "twosvc.Beta/Echo", ~s({"text": "x"}))
    assert out =~ ~s("text": "beta:x")
  end

  test "server streaming works", ctx do
    assert {out, 0} = grpcurl(ctx, "twosvc.Alpha/Count", ~s({"text": "n"}))
    assert out =~ ~s("text": "n1")
    assert out =~ ~s("text": "n3")
  end

  test "a crashing handler is INTERNAL", ctx do
    # grpcurl exits with 64 + the gRPC status code
    assert {out, 77} = grpcurl(ctx, "twosvc.Alpha/Boom", ~s({"text": "x"}))
    assert out =~ "Internal"
  end
end
