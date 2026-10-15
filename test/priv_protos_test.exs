defmodule SimpleGrpc.PrivProtosTest do
  use ExUnit.Case, async: true

  # Everything under priv/ is published to hex and the library's own priv/protos is an
  # implicit import search root, so only protos the library itself needs at runtime may
  # live there. Test fixtures belong in test/fixtures/protos.
  @runtime_protos ~w(
    grpc/health/v1/health.proto
    grpc/reflection/v1/reflection.proto
    grpc/reflection/v1alpha/reflection.proto
  )

  test "priv/protos contains only the protos the library needs at runtime" do
    found =
      "priv/protos/**/*.proto"
      |> Path.wildcard()
      |> Enum.map(&Path.relative_to(&1, "priv/protos"))
      |> Enum.sort()

    assert found == Enum.sort(@runtime_protos),
           "move test-only protos to test/fixtures/protos (or update @runtime_protos " <>
             "if the library now needs a new runtime proto)"
  end
end
