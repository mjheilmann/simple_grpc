defmodule SimpleGrpc.ServerFilesGlobTest do
  use ExUnit.Case, async: true

  @pinger """
  syntax = "proto3";
  package glob_a;
  service Pinger { rpc Ping(PingRequest) returns (PingReply); }
  message PingRequest {}
  message PingReply {}
  """

  @other """
  syntax = "proto3";
  package glob_b;
  message Other { string name = 1; }
  """

  @extra """
  syntax = "proto3";
  package glob_c;
  message Extra { string name = 1; }
  """

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp} do
    File.write!(Path.join(tmp, "a.proto"), @pinger)
    File.write!(Path.join(tmp, "b.proto"), @other)
    File.write!(Path.join(tmp, "notes.txt"), "not a proto file")
    File.mkdir_p!(Path.join(tmp, "dir_named.proto"))
    File.mkdir_p!(Path.join(tmp, "sub"))
    File.write!(Path.join([tmp, "sub", "c.proto"]), @extra)
    :ok
  end

  describe "glob entries in files:" do
    test "expand to the .proto files that match, skipping directories and other files",
         %{tmp_dir: tmp} do
      mod = compile_server(~s(["#{tmp}/*"]))

      assert mod.__schema__().files == [Path.join(tmp, "a.proto"), Path.join(tmp, "b.proto")]
    end

    test "can reach into subdirectories", %{tmp_dir: tmp} do
      mod = compile_server(~s(["#{tmp}/a.proto", "#{tmp}/*/*.proto"]))

      assert mod.__schema__().files == [
               Path.join(tmp, "a.proto"),
               Path.join([tmp, "sub", "c.proto"])
             ]

      assert Map.has_key?(mod.__schema__().messages, "glob_c.Extra")
    end

    test "keep list order, sort each glob's matches, and list a file once", %{tmp_dir: tmp} do
      mod = compile_server(~s(["#{tmp}/b.proto", "#{tmp}/*.proto", "#{tmp}/a.proto"]))

      assert mod.__schema__().files == [Path.join(tmp, "b.proto"), Path.join(tmp, "a.proto")]
    end

    test "that match nothing are a compile error naming the pattern", %{tmp_dir: tmp} do
      pattern = "#{tmp}/nothing/*.proto"

      error =
        assert_raise CompileError, fn -> compile_server(~s(["#{tmp}/a.proto", "#{pattern}"])) end

      assert error.description =~ "matched no .proto files"
      assert error.description =~ pattern
    end

    test "work with the test fixtures end to end" do
      mod =
        compile_server(~s(["test/fixtures/protos/multi_file/{greeter,types}.proto"]),
          service: "Greeter",
          handlers: "def say_hello(_req, _stream), do: {:ok, %{}}"
        )

      assert mod.__schema__().files == [
               "test/fixtures/protos/multi_file/greeter.proto",
               "test/fixtures/protos/multi_file/types.proto"
             ]

      assert Map.has_key?(mod.__schema__().messages, "multi_file_types.Person")
    end
  end

  describe "__mix_recompile__?/0" do
    test "turns true when a file appears or disappears under a glob", %{tmp_dir: tmp} do
      mod = compile_server(~s(["#{tmp}/*.proto"]))

      refute mod.__mix_recompile__?()

      added = Path.join(tmp, "added.proto")
      File.write!(added, @extra)
      assert mod.__mix_recompile__?()

      File.rm!(added)
      refute mod.__mix_recompile__?()

      File.rm!(Path.join(tmp, "b.proto"))
      assert mod.__mix_recompile__?()
    end

    test "is not defined for servers without glob entries", %{tmp_dir: tmp} do
      mod = compile_server(~s(["#{tmp}/a.proto"]))

      refute function_exported?(mod, :__mix_recompile__?, 0)
    end
  end

  defp compile_server(files_source, opts \\ []) do
    name = Module.concat(__MODULE__, "Server#{System.unique_integer([:positive])}")
    service = if opts[:service], do: ", service: #{inspect(opts[:service])}", else: ""
    handlers = opts[:handlers] || "def ping(_req, _stream), do: {:ok, %{}}"

    Code.compile_string("""
    defmodule #{inspect(name)} do
      use SimpleGrpc.Server, files: #{files_source}#{service}
      #{handlers}
    end
    """)

    name
  end
end
