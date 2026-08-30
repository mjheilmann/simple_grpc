defmodule SimpleGrpc.ServerTest do
  use ExUnit.Case

  test "Server Compile fails when no service is present" do
    server = """
    defmodule TestService do
      use SimpleGrpc.Server, 
      server_file: "priv/protos/common/common_types.proto"
    end
    """

    expected_error =
      "no match of right hand side value:\n\n    {:error, \"service definition not found\"}\n"

    assert_raise MatchError, expected_error, fn ->
      Code.eval_string(server)
    end
  end

  test "Server Compile succeeds when a service is present" do
    server = """
    defmodule TestService do
      use SimpleGrpc.Server, 
      server_file: "priv/protos/empty_service/empty_service.proto"
    end
    """

    Code.eval_string(server)
  end
end
