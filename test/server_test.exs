defmodule SimpleGrpc.ServerTest do
  use ExUnit.Case

  defmodule TestService do
    use SimpleGrpc.Server,
      # server_file: "../grpc-reflection/priv/protos/common/common_types.proto"

      # server_file: "../grpc-reflection/priv/protos/custom_prefix/custom_prefix_service.proto"

      server_file: "../grpc-reflection/priv/protos/deprecated_fields/deprecated_fields.proto"

    # server_file: "../grpc-reflection/priv/protos/edge_cases/edge_cases.proto"
    # server_file: "../grpc-reflection/priv/protos/empty_service/empty_service.proto"
    # server_file: "../grpc-reflection/priv/protos/global_service.proto"
    # server_file: "../grpc-reflection/priv/protos/grpc/reflection/v1/reflection.proto"
    # server_file: "../grpc-reflection/priv/protos/grpc/reflection/v1alpha/reflection.proto"
    # server_file: "../grpc-reflection/priv/protos/imports_test/imports_test.proto"
    # server_file: "../grpc-reflection/priv/protos/nested_enum_conflict/nested_enum_conflict.proto"
    # server_file: "../grpc-reflection/priv/protos/nested/nested_messages.proto"
    # server_file: "../grpc-reflection/priv/protos/package_a/package_a.proto"
    # server_file: "../grpc-reflection/priv/protos/package_b/package_b.proto"
    # server_file: "../grpc-reflection/priv/protos/recursive_message/recursive_message.proto"
    # server_file: "../grpc-reflection/priv/protos/scalar_types/scalar_types.proto"
    # server_file: "../grpc-reflection/priv/protos/streaming/streaming_service.proto"
    # server_file: "../grpc-reflection/priv/protos/well_known_tyoes/well_known_types.proto"
  end
end
