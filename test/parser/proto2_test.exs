defmodule SimpleGrpc.Parser.Proto2Test do
  use ExUnit.Case

  @known_fields [:syntax, :package, :import, :service, :message, :enum, :option, :extend]

  defp parse_proto!(rel_path) do
    path = Path.expand("priv/protos/#{rel_path}", File.cwd!())
    contents = File.read!(path)
    {:ok, ast, "", %{}, _, _} = SimpleGrpc.Parser.Proto2.parse(contents)
    ast
  end

  defp assert_no_unhandled(ast, label) do
    unhandled = Keyword.drop(ast, @known_fields)
    assert unhandled == [], "Unhandled tokens found in #{label}: #{inspect(unhandled)}"
  end

  test "parses proto2_features/proto2_features.proto" do
    ast = parse_proto!("proto2_features/proto2_features.proto")
    assert_no_unhandled(ast, "proto2_features.proto")

    assert {:syntax, "proto2"} in ast
    assert {:package, "proto2_features"} in ast
  end

  test "parses required, optional, repeated and default field options" do
    proto = """
    syntax = "proto2";
    package test;

    message TestDefaults {
      required string req = 1;
      optional int32 port = 2 [default = 8080];
      optional string host = 3 [default = "localhost"];
      optional bool enabled = 4 [default = true];
      optional float ratio = 5 [default = 1.5];
      optional int32 neg = 6 [default = -42];
      repeated int32 items = 7 [packed = true];
    }
    """

    assert {:ok, ast, "", %{}, _, _} = SimpleGrpc.Parser.Proto2.parse(proto)
    assert_no_unhandled(ast, "TestDefaults")

    {:message, ["TestDefaults" | items]} = List.keyfind(ast, :message, 0)
    req_field = Enum.find(items, fn {:field, f} -> "req" in f end)
    assert match?({:field, [{:prefix, :required}, "string", "req", 1 | _]}, req_field)

    port_field = Enum.find(items, fn {:field, f} -> "port" in f end)

    assert match?(
             {:field, [{:prefix, :optional}, "int32", "port", 2, {:field_options, _}]},
             port_field
           )
  end

  test "parses extensions and extend blocks" do
    proto = """
    syntax = "proto2";
    package test;

    message BaseMsg {
      required int32 id = 1;
      extensions 100 to 199, 1000 to max;
    }

    extend BaseMsg {
      optional string extra = 100 [default = "none"];
    }
    """

    assert {:ok, ast, "", %{}, _, _} = SimpleGrpc.Parser.Proto2.parse(proto)
    assert_no_unhandled(ast, "Extensions")

    {:message, ["BaseMsg" | items]} = List.keyfind(ast, :message, 0)
    assert {:extensions, [{100, 199}, {1000, :max}]} in items

    extend_item = List.keyfind(ast, :extend, 0)

    assert match?(
             {:extend, ["BaseMsg", {:field, [{:prefix, :optional}, "string", "extra", 100 | _]}]},
             extend_item
           )
  end

  test "parses legacy proto2 group syntax" do
    proto = """
    syntax = "proto2";
    package test;

    message WithGroup {
      required int32 id = 1;
      optional group Result = 2 {
        required string url = 1;
        optional string title = 2;
      }
    }
    """

    assert {:ok, ast, "", %{}, _, _} = SimpleGrpc.Parser.Proto2.parse(proto)
    assert_no_unhandled(ast, "WithGroup")
  end
end
