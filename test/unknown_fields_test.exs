defmodule SimpleGrpc.UnknownFieldsTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Protobuf

  test "unknown fields are preserved during decode and re-emitted on encode" do
    # V2 schema with extra fields
    proto_v2 = """
    syntax = "proto3";
    package test.unknown;

    message Payload {
      string id = 1;
      int32 count = 2;
      string extra_info = 10;
    }
    """

    # V1 schema without extra_info
    proto_v1 = """
    syntax = "proto3";
    package test.unknown;

    message Payload {
      string id = 1;
      int32 count = 2;
    }
    """

    assert {:ok, schema_v2} = Schema.from_string(proto_v2)
    assert {:ok, schema_v1} = Schema.from_string(proto_v1)

    v2_msg_desc = Schema.message(schema_v2, "test.unknown.Payload")
    v1_msg_desc = Schema.message(schema_v1, "test.unknown.Payload")

    # Encode with v2 schema
    original_data = %{id: "order-123", count: 42, extra_info: "secret-feature-flag"}
    {:ok, v2_binary} = Protobuf.encode(v2_msg_desc, original_data, schema_v2)

    # Decode with v1 schema (simulating proxy or older client)
    {:ok, v1_decoded} = Protobuf.decode(v1_msg_desc, v2_binary, schema_v1)
    assert v1_decoded.id == "order-123"
    assert v1_decoded.count == 42
    assert is_list(v1_decoded[:__unknown_fields__])
    assert length(v1_decoded[:__unknown_fields__]) > 0

    # Re-encode with v1 schema (pass-through / mutate v1 fields)
    updated_v1 = Map.put(v1_decoded, :count, 43)
    {:ok, re_encoded_binary} = Protobuf.encode(v1_msg_desc, updated_v1, schema_v1)

    # Decode again with v2 schema to confirm extra_info was retained!
    {:ok, v2_recovered} = Protobuf.decode(v2_msg_desc, re_encoded_binary, schema_v2)
    assert v2_recovered.id == "order-123"
    assert v2_recovered.count == 43
    assert v2_recovered.extra_info == "secret-feature-flag"
  end
end
