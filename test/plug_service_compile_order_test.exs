defmodule SimpleGrpc.PlugServiceCompileOrderTest do
  use ExUnit.Case, async: false

  # `Plug.Builder` runs `Plug.Service.init/1` while the pipeline module compiles, so the server
  # module it names must be waited for when both are compiled in the same project (in parallel).
  @tag :tmp_dir
  test "a pipeline compiles even when its file is compiled before the server's", %{
    tmp_dir: dir
  } do
    pipeline = Path.join(dir, "a_pipeline.ex")
    server = Path.join(dir, "z_server.ex")

    File.write!(pipeline, """
    defmodule CompileOrder.Pipeline do
      use Plug.Builder

      plug SimpleGrpc.Plug.Service, server: CompileOrder.Server
      plug SimpleGrpc.Plug.NotFound
    end
    """)

    File.write!(server, """
    defmodule CompileOrder.Server do
      use SimpleGrpc.Server,
        files: ["test/fixtures/protos/two_services/two_services.proto"],
        service: "Beta"

      def echo(req, _ctx), do: {:ok, req}
      def only_beta(req, _ctx), do: {:ok, req}
    end
    """)

    # pipeline file first, so its module is reached before the server's exists
    assert {:ok, modules, _warnings} = Kernel.ParallelCompiler.compile([pipeline, server])
    assert CompileOrder.Pipeline in modules

    on_exit(fn ->
      for module <- modules do
        :code.purge(module)
        :code.delete(module)
      end
    end)
  end
end
