defmodule SimpleGrpc.SchemaPruneScopeTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Schema

  test "pruning to a service keeps the definitions its types resolve to" do
    {:ok, schema} = Schema.from_file("test/fixtures/protos/enum_scope/prune.proto")
    {:ok, pruned} = Schema.filter_for_service(schema, "Pruned")

    full_names = fn definitions ->
      definitions |> Map.values() |> Enum.map(& &1.full_name) |> Enum.uniq() |> Enum.sort()
    end

    assert full_names.(pruned.enums) == ["enumprune.Reached.Type"]

    assert full_names.(pruned.messages) ==
             ["enumprune.Outer1", "enumprune.Outer1.Item", "enumprune.Reached"]
  end
end
