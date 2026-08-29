defmodule SimpleGrpc.Parser.Proto3Test do
  use ExUnit.Case

  @known_fields [:syntax, :package, :import, :service, :message, :enum, :option]

  defp parse_proto!(rel_path) do
    path = Path.expand("../grpc-reflection/priv/protos/#{rel_path}", File.cwd!())
    contents = File.read!(path)
    {:ok, ast, "", %{}, _, _} = SimpleGrpc.Parser.Proto3.parse(contents)
    ast
  end

  defp assert_no_unhandled(ast, label) do
    unhandled = Keyword.drop(ast, @known_fields)
    assert unhandled == [], "Unhandled tokens found in #{label}: #{inspect(unhandled)}"
  end

  test "parses common/common_types.proto" do
    ast = parse_proto!("common/common_types.proto")
    assert_no_unhandled(ast, "common_types.proto")
  end

  test "parses custom_prefix/custom_prefix_service.proto" do
    ast = parse_proto!("custom_prefix/custom_prefix_service.proto")
    assert_no_unhandled(ast, "custom_prefix_service.proto")
  end

  test "parses deprecated_fields/deprecated_fields.proto" do
    ast = parse_proto!("deprecated_fields/deprecated_fields.proto")
    assert_no_unhandled(ast, "deprecated_fields.proto")
  end

  test "parses edge_cases/edge_cases.proto" do
    ast = parse_proto!("edge_cases/edge_cases.proto")
    assert_no_unhandled(ast, "edge_cases.proto")
  end

  test "parses empty_service/empty_service.proto" do
    ast = parse_proto!("empty_service/empty_service.proto")
    assert_no_unhandled(ast, "empty_service.proto")
  end

  test "parses global_service.proto" do
    ast = parse_proto!("global_service.proto")
    assert_no_unhandled(ast, "global_service.proto")
  end

  test "parses grpc/reflection/v1/reflection.proto" do
    ast = parse_proto!("grpc/reflection/v1/reflection.proto")
    assert_no_unhandled(ast, "reflection.proto (v1)")
  end

  test "parses grpc/reflection/v1alpha/reflection.proto" do
    ast = parse_proto!("grpc/reflection/v1alpha/reflection.proto")
    assert_no_unhandled(ast, "reflection.proto (v1alpha)")
  end

  test "parses imports_test/imports_test.proto" do
    ast = parse_proto!("imports_test/imports_test.proto")
    assert_no_unhandled(ast, "imports_test.proto")
  end

  test "parses nested_enum_conflict/nested_enum_conflict.proto" do
    ast = parse_proto!("nested_enum_conflict/nested_enum_conflict.proto")
    assert_no_unhandled(ast, "nested_enum_conflict.proto")
  end

  test "parses nested/nested_messages.proto" do
    ast = parse_proto!("nested/nested_messages.proto")
    assert_no_unhandled(ast, "nested_messages.proto")
  end

  test "parses package_a/package_a.proto" do
    ast = parse_proto!("package_a/package_a.proto")
    assert_no_unhandled(ast, "package_a.proto")
  end

  test "parses package_b/package_b.proto" do
    ast = parse_proto!("package_b/package_b.proto")
    assert_no_unhandled(ast, "package_b.proto")
  end

  test "parses recursive_message/recursive_message.proto" do
    ast = parse_proto!("recursive_message/recursive_message.proto")
    assert_no_unhandled(ast, "recursive_message.proto")
  end

  test "parses scalar_types/scalar_types.proto" do
    ast = parse_proto!("scalar_types/scalar_types.proto")
    assert_no_unhandled(ast, "scalar_types.proto")
  end

  test "parses streaming/streaming_service.proto" do
    ast = parse_proto!("streaming/streaming_service.proto")
    assert_no_unhandled(ast, "streaming_service.proto")
  end

  test "parses well_known_tyoes/well_known_types.proto" do
    ast = parse_proto!("well_known_tyoes/well_known_types.proto")
    assert_no_unhandled(ast, "well_known_types.proto")
  end
end
