defmodule SimpleGrpc.ServerFilesTest do
  use SimpleGrpc.Case, plug: SimpleGrpc.Plug

  alias SimpleGrpc.Protobuf

  @greeter "test/fixtures/protos/multi_file/greeter.proto"
  @types "test/fixtures/protos/multi_file/types.proto"

  defmodule GreeterServer do
    use SimpleGrpc.Server,
      files: [
        "test/fixtures/protos/multi_file/greeter.proto",
        "test/fixtures/protos/multi_file/types.proto"
      ],
      service: "Greeter"

    def say_hello(req, _stream) do
      {:ok,
       %{
         message: "Hello, " <> req.name,
         person: %{name: req.name, age: 30},
         friends: [%{name: "Ada", age: 36}]
       }}
    end
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

  defmodule SingleFileServer do
    use SimpleGrpc.Server, server_file: "test/fixtures/protos/empty_service/empty_service.proto"
  end

  defmodule WildcardServer do
    use SimpleGrpc.Server,
      files: Path.wildcard("test/fixtures/protos/multi_file/{greeter,types}.proto"),
      service: "Greeter"

    def say_hello(_req, _stream), do: {:ok, %{}}
  end

  describe "files: with imports across files" do
    test "schema contains messages from every listed file" do
      schema = GreeterServer.__schema__()

      assert Map.has_key?(schema.messages, "multi_file.HelloReply")
      assert Map.has_key?(schema.messages, "multi_file_types.Person")
    end

    test "schema.files keeps the listed paths as given (no absolute paths)" do
      assert GreeterServer.__schema__().files == [@greeter, @types]
    end

    test "round-trips a message that contains a type from an imported file", %{conn: conn} do
      SimpleGrpc.Registry.clear()
      SimpleGrpc.register_service(GreeterServer)
      schema = GreeterServer.__schema__()

      req = Protobuf.encode!("HelloRequest", %{name: "Bob"}, schema)

      {:ok, resp_bytes, _headers, _trailers} =
        grpc_call(conn, "/multi_file.Greeter/SayHello", req)

      {:ok, reply} = Protobuf.decode("HelloReply", resp_bytes, schema)

      assert reply.message == "Hello, Bob"
      assert reply.person.name == "Bob"
      assert reply.person.age == 30
      assert [%{name: "Ada", age: 36}] = reply.friends
    end

    test "records every listed file as an external resource" do
      resources =
        GreeterServer.module_info(:attributes)
        |> Keyword.get_values(:external_resource)
        |> List.flatten()

      assert Enum.any?(resources, &String.ends_with?(&1, "multi_file/greeter.proto"))
      assert Enum.any?(resources, &String.ends_with?(&1, "multi_file/types.proto"))
    end
  end

  describe "service selection" do
    test "service: picks one of several services defined in the files" do
      assert GreeterServer.__service_name__() == "Greeter"
      assert AdminServer.__service_name__() == "Admin"
      assert Enum.map(AdminServer.__rpcs__(), & &1.name) == ["Ping"]
    end

    test "several services without service: is a compile error listing the candidates" do
      error =
        assert_raise CompileError, fn ->
          compile("""
          defmodule SimpleGrpc.ServerFilesTest.NoServiceChosen do
            use SimpleGrpc.Server, files: ["#{@greeter}", "#{@types}"]
          end
          """)
        end

      assert error.description =~ "Greeter"
      assert error.description =~ "Admin"
      assert error.description =~ "service:"
    end

    test "the same applies to services in a single server_file:" do
      error =
        assert_raise CompileError, fn ->
          compile("""
          defmodule SimpleGrpc.ServerFilesTest.NoServiceChosenSingle do
            use SimpleGrpc.Server, server_file: "test/fixtures/protos/streaming/streaming_service.proto"
          end
          """)
        end

      assert error.description =~ "StreamingService"
      assert error.description =~ "MultiStreamService"
    end

    test "a single service needs no service:" do
      assert SingleFileServer.__service_name__() == "EmptyService"
    end

    test "unknown service: is a compile error listing what is available" do
      error =
        assert_raise CompileError, fn ->
          compile("""
          defmodule SimpleGrpc.ServerFilesTest.UnknownService do
            use SimpleGrpc.Server, files: ["#{@greeter}", "#{@types}"], service: "Nope"
          end
          """)
        end

      assert error.description =~ "Nope"
      assert error.description =~ "Greeter"
      assert error.description =~ "Admin"
    end
  end

  describe "files: as a compile-time expression" do
    test "accepts Path.wildcard/1" do
      assert WildcardServer.__schema__().files == [@greeter, @types]
      assert Map.has_key?(WildcardServer.__schema__().messages, "multi_file_types.Person")
    end
  end

  describe "server_file: shorthand" do
    test "still works for a single self-contained file" do
      assert SingleFileServer.__schema__().files == [
               "test/fixtures/protos/empty_service/empty_service.proto"
             ]
    end
  end

  describe "import validation" do
    test "an import that is not in the list is a compile error naming file and import" do
      error =
        assert_raise CompileError, fn ->
          compile("""
          defmodule SimpleGrpc.ServerFilesTest.MissingImport do
            use SimpleGrpc.Server, files: ["#{@greeter}"]
          end
          """)
        end

      assert error.description =~ "multi_file/types.proto"
      assert error.description =~ "greeter.proto"
    end

    test "an import matching more than one listed file is a compile error" do
      error =
        assert_raise CompileError, fn ->
          compile("""
          defmodule SimpleGrpc.ServerFilesTest.AmbiguousImport do
            use SimpleGrpc.Server,
              files: [
                "test/fixtures/protos/multi_file/ambiguous_import.proto",
                "test/fixtures/protos/multi_file/dup_a/types.proto",
                "test/fixtures/protos/multi_file/dup_b/types.proto"
              ]
          end
          """)
        end

      assert error.description =~ "ambiguous"
      assert error.description =~ "dup_a/types.proto"
      assert error.description =~ "dup_b/types.proto"
    end

    test "an unresolvable import in a file on disk is reported by import string" do
      error =
        assert_raise CompileError, fn ->
          compile("""
          defmodule SimpleGrpc.ServerFilesTest.NoSuchImport do
            use SimpleGrpc.Server, files: ["test/fixtures/protos/multi_file/missing_import.proto"]
          end
          """)
        end

      assert error.description =~ "multi_file/does_not_exist.proto"
    end
  end

  describe "duplicate definitions" do
    test "the same fully-qualified name in two listed files is a compile error naming both" do
      error =
        assert_raise CompileError, fn ->
          compile("""
          defmodule SimpleGrpc.ServerFilesTest.Duplicate do
            use SimpleGrpc.Server,
              files: [
                "test/fixtures/protos/multi_file/greeter.proto",
                "test/fixtures/protos/multi_file/types.proto",
                "test/fixtures/protos/multi_file/dup_a/types.proto",
                "test/fixtures/protos/multi_file/dup_b/types.proto"
              ],
              service: "Greeter"
          end
          """)
        end

      assert error.description =~ "dup.Thing"
      assert error.description =~ "dup_a/types.proto"
      assert error.description =~ "dup_b/types.proto"
    end
  end

  describe "option validation" do
    test "giving both files: and server_file: is a compile error" do
      assert_raise CompileError, ~r/files:.*server_file:|server_file:.*files:/, fn ->
        compile("""
        defmodule SimpleGrpc.ServerFilesTest.Both do
          use SimpleGrpc.Server, files: ["#{@greeter}"], server_file: "#{@greeter}"
        end
        """)
      end
    end

    test "giving neither is a compile error" do
      assert_raise CompileError, ~r/files:/, fn ->
        compile("""
        defmodule SimpleGrpc.ServerFilesTest.Neither do
          use SimpleGrpc.Server
        end
        """)
      end
    end
  end

  defp compile(source), do: Code.compile_string(source)
end
