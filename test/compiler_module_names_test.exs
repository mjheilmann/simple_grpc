defmodule SimpleGrpc.CompilerModuleNamesTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias SimpleGrpc.Schema

  @proto """
  syntax = "proto3";
  package naming;
  message Thing { string name = 1; }
  """

  @other_proto """
  syntax = "proto3";
  package naming;
  message Thing { string name = 1; int32 size = 2; }
  """

  test "a schema always compiles to the same module name" do
    {:ok, first} = Schema.from_string(@proto)
    {:ok, second} = Schema.from_string(@proto)

    assert first.compiled_module
    assert first.compiled_module == second.compiled_module
  end

  test "an identical schema reuses the loaded module instead of redefining it" do
    {:ok, _} = Schema.from_string(@proto)

    output = capture_io(:stderr, fn -> {:ok, _} = Schema.from_string(@proto) end)

    refute output =~ "redefining"
  end

  test "different schemas get different modules" do
    {:ok, one} = Schema.from_string(@proto)
    {:ok, other} = Schema.from_string(@other_proto)

    assert one.compiled_module != other.compiled_module

    assert {:ok, %{name: "x"}} = SimpleGrpc.Protobuf.decode("Thing", <<10, 1, ?x>>, one)

    assert {:ok, %{name: "x", size: 0}} =
             SimpleGrpc.Protobuf.decode("Thing", <<10, 1, ?x>>, other)
  end

  test "merging the same schemas twice gives the same module" do
    {:ok, a} = Schema.from_string(@proto)
    {:ok, b} = Schema.from_string(@other_proto)

    assert Schema.merge([a, b]).compiled_module == Schema.merge([a, b]).compiled_module
  end

  test "processes compiling the same schema at once all get a working codec" do
    package = "concurrent#{System.unique_integer([:positive])}"

    proto = """
    syntax = "proto3";
    package #{package};
    message Thing { string name = 1; }
    """

    output =
      capture_io(:stderr, fn ->
        schemas =
          1..20
          |> Task.async_stream(fn _ -> Schema.from_string(proto) end, timeout: 30_000)
          |> Enum.map(fn {:ok, {:ok, schema}} -> schema end)

        send(self(), {:schemas, schemas})
      end)

    assert_received {:schemas, schemas}
    refute output =~ "cannot define"
    assert [module] = schemas |> Enum.map(& &1.compiled_module) |> Enum.uniq()
    assert module

    assert {:ok, %{name: "x"}} =
             SimpleGrpc.Protobuf.decode("#{package}.Thing", <<10, 1, ?x>>, hd(schemas))
  end

  test "the bundled well-known type modules are never redefined by a later compile" do
    timestamp = Schema.WellKnownTypes.schema_for("google/protobuf/timestamp.proto")
    duration = Schema.WellKnownTypes.schema_for("google/protobuf/duration.proto")
    before_md5 = timestamp.compiled_module.module_info(:md5)

    output =
      capture_io(:stderr, fn ->
        for _ <- 1..20, do: Schema.merge([timestamp, duration])
      end)

    refute output =~ "redefining"
    assert timestamp.compiled_module.module_info(:md5) == before_md5
  end
end
