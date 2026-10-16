defmodule SimpleGrpc.ServerMessageModulesTest do
  use ExUnit.Case, async: true

  @greeter_files [
    "test/fixtures/protos/multi_file/greeter.proto",
    "test/fixtures/protos/multi_file/types.proto"
  ]

  defmodule GreeterServer do
    use SimpleGrpc.Server,
      files: [
        "test/fixtures/protos/multi_file/greeter.proto",
        "test/fixtures/protos/multi_file/types.proto"
      ],
      service: "Greeter"

    def say_hello(_req, _stream), do: {:ok, %{}}
  end

  defmodule SwapperServer do
    use SimpleGrpc.Server,
      files: ["test/fixtures/protos/multi_file/same_name/*.proto"]

    def swap(req, _stream), do: {:ok, %{b: req.a}}
    def wrap(req, _stream), do: {:ok, %{item: %{b: req.item.a}}}
    def nest(req, _stream), do: {:ok, req}
  end

  defmodule AdminServer do
    use SimpleGrpc.Server,
      files: [
        "test/fixtures/protos/multi_file/greeter.proto",
        "test/fixtures/protos/multi_file/types.proto"
      ],
      service: "Admin"

    def ping(_req, _stream), do: {:ok, %{ok: true}}
  end

  defmodule PrunerServer do
    use SimpleGrpc.Server, files: ["test/fixtures/protos/multi_file/prune/*.proto"]

    def run(_req, _stream), do: {:ok, %{}}
  end

  defmodule EmptyInputServer do
    use SimpleGrpc.Server, server_file: "test/fixtures/protos/multi_file/empty_input.proto"

    def ping(_req, _stream), do: {:ok, %{ok: true}}
  end

  describe "a struct module for every message the service reaches" do
    test "messages in the service file are qualified by their proto package" do
      assert Code.ensure_loaded?(GreeterServer.MultiFile.HelloRequest)
      assert Code.ensure_loaded?(GreeterServer.MultiFile.HelloReply)

      assert %GreeterServer.MultiFile.HelloRequest{name: nil} =
               struct(GreeterServer.MultiFile.HelloRequest)
    end

    test "messages from imported files in other packages get structs too" do
      assert Code.ensure_loaded?(GreeterServer.MultiFileTypes.Person)

      assert [:age, :name] ==
               GreeterServer.MultiFileTypes.Person
               |> struct()
               |> Map.keys()
               |> Kernel.--([:__struct__, :__unknown_fields__])
               |> Enum.sort()
    end

    test "the same short name in two packages gives two distinct modules" do
      one = SwapperServer.SameOne.Item
      two = SwapperServer.SameTwo.Item

      assert Code.ensure_loaded?(one)
      assert Code.ensure_loaded?(two)
      assert Map.has_key?(struct(one), :a)
      assert Map.has_key?(struct(two), :b)
    end

    test "message modules still know the server's schema (for enum and field coercion)" do
      assert GreeterServer.MultiFile.HelloRequest.__schema__() == GreeterServer.__schema__()
      assert %{name: "Bob"} = GreeterServer.MultiFile.HelloRequest.new(%{name: "Bob"})
    end
  end

  describe "__messages__/0" do
    test "lists every top-level message by fully-qualified name" do
      messages = GreeterServer.__messages__()

      assert messages["multi_file.HelloRequest"] == GreeterServer.MultiFile.HelloRequest
      assert messages["multi_file_types.Person"] == GreeterServer.MultiFileTypes.Person
    end

    test "also answers to the short name of messages in the service's own package" do
      messages = GreeterServer.__messages__()

      assert messages["HelloRequest"] == GreeterServer.MultiFile.HelloRequest
      refute Map.has_key?(messages, "Person")
    end

    test "build_request/2 and build_response/2 use the qualified modules" do
      assert %GreeterServer.MultiFile.HelloRequest{name: "Ann"} =
               GreeterServer.build_request(:say_hello, %{name: "Ann"})

      assert %GreeterServer.MultiFile.HelloReply{message: "hi"} =
               GreeterServer.build_response(:say_hello, %{message: "hi"})

      assert %GreeterServer.MultiFile.HelloRequest{} =
               GreeterServer.build_request("HelloRequest", %{})
    end
  end

  describe "RPC input and output types" do
    test "resolve to the module of a message from another package" do
      info = SwapperServer.route_info("/same_svc.Swapper/Swap")

      assert info.input_module == SwapperServer.SameOne.Item
      assert info.output_module == SwapperServer.SameTwo.Item
    end

    test "a bundled well-known type with a struct resolves to the library's module" do
      info = EmptyInputServer.route_info("/emp.Pinger/Ping")

      assert info.input_module == SimpleGrpc.WKT.Empty
      assert info.output_module == EmptyInputServer.Emp.PingReply
    end

    test "an unknown type is a compile error naming the rpc and the type" do
      error =
        assert_raise CompileError, fn ->
          compile("""
          defmodule SimpleGrpc.ServerMessageModulesTest.UnknownType do
            use SimpleGrpc.Server, server_file: "test/fixtures/protos/multi_file/unknown_type.proto"
          end
          """)
        end

      assert error.description =~ "Find"
      assert error.description =~ "Nope"
    end
  end

  describe "only reachable messages get structs" do
    test "messages of the other service in the same file are left out" do
      refute Code.ensure_loaded?(GreeterServer.MultiFile.PingRequest)
      refute Map.has_key?(GreeterServer.__messages__(), "multi_file.PingReply")
      assert Code.ensure_loaded?(AdminServer.MultiFile.PingRequest)
      refute Code.ensure_loaded?(AdminServer.MultiFileTypes.Person)
    end

    test "follows map values, oneofs, repeated, nested and cross-package fields" do
      keys = PrunerServer.__messages__() |> Map.keys() |> Enum.sort()

      assert keys == [
               "Entry",
               "Leaf",
               "Outer",
               "Request",
               "Response",
               "prune.Entry",
               "prune.Leaf",
               "prune.Outer",
               "prune.Request",
               "prune.Response",
               "prune_other.Shared"
             ]
    end

    test "unreachable messages do not exist as modules" do
      refute Code.ensure_loaded?(PrunerServer.Prune.Unused)
      refute Code.ensure_loaded?(PrunerServer.PruneOther.AlsoUnused)
      refute Code.ensure_loaded?(PrunerServer.PruneUnused.Stranger)
    end

    test "a nested message is generated inside its reached parent" do
      assert Code.ensure_loaded?(PrunerServer.Prune.Outer.Child)
      assert Code.ensure_loaded?(PrunerServer.Prune.Outer.Sibling)
    end

    test "the schema still describes every listed message" do
      assert Map.has_key?(PrunerServer.__schema__().messages, "prune_unused.Stranger")
    end

    test "build_request/2 rejects a message that was left out" do
      assert_raise ArgumentError, fn -> GreeterServer.build_request("PingRequest", %{}) end
    end
  end

  describe "module name collisions" do
    test "colliding messages the service cannot reach are ignored" do
      compile("""
      defmodule SimpleGrpc.ServerMessageModulesTest.UnusedCollision do
        use SimpleGrpc.Server, files: ["test/fixtures/protos/multi_file/collision_unused/*.proto"]
        def run(req, _stream), do: {:ok, req}
      end
      """)

      messages = apply(SimpleGrpc.ServerMessageModulesTest.UnusedCollision, :__messages__, [])
      assert messages |> Map.keys() |> Enum.sort() == ["Thing", "col_x.Thing"]
    end

    test "two reachable packages that map to the same module name are a compile error" do
      error =
        assert_raise CompileError, fn ->
          compile("""
          defmodule SimpleGrpc.ServerMessageModulesTest.Collision do
            use SimpleGrpc.Server, files: ["test/fixtures/protos/multi_file/collision/*.proto"]
          end
          """)
        end

      assert error.description =~ "col_x.Thing"
      assert error.description =~ "colX.Thing"
      assert error.description =~ "ColX.Thing"
    end
  end

  test "fixtures list is what the greeter server uses" do
    assert GreeterServer.__schema__().files == @greeter_files
  end

  defp compile(source), do: Code.compile_string(source)
end
