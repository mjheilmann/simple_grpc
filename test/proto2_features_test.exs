defmodule SimpleGrpc.Proto2FeaturesTest do
  use ExUnit.Case

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Reflection.Descriptor

  @proto_path "priv/protos/proto2_features/proto2_features.proto"

  setup_all do
    {:ok, schema} = Schema.from_file(@proto_path)
    [schema: schema]
  end

  test "loads proto2 schema and attaches extension fields", %{schema: schema} do
    assert schema.syntax == :proto2
    assert schema.package == "proto2_features"

    # Verify Proto2Request has extension fields attached
    request_msg = Schema.message(schema, "Proto2Request")
    assert request_msg != nil

    assert Schema.Message.field_by_number(request_msg, 100) != nil
    assert Schema.Message.field_by_name(request_msg, :extended_field) != nil

    assert Schema.Message.field_by_number(request_msg, 101) != nil
    assert Schema.Message.field_by_name(request_msg, :extended_timestamp) != nil

    assert Schema.Message.field_by_number(request_msg, 102) != nil
    assert Schema.Message.field_by_name(request_msg, :extension_data) != nil

    # Verify required fields
    assert :required_field in request_msg.required_fields
    assert :required_id in request_msg.required_fields
  end

  test "encodes and decodes proto2 message with defaults and presence", %{schema: schema} do
    request_msg = Schema.message(schema, "Proto2Request")

    # When optional fields are omitted, explicit defaults are applied on decode,
    # while optional fields without default remain nil (presence semantics)
    data = %{
      required_field: "hello",
      required_id: 42
    }

    assert {:ok, encoded} = Protobuf.encode(request_msg, data, schema)
    assert {:ok, decoded} = Protobuf.decode(request_msg, encoded, schema)

    assert decoded.required_field == "hello"
    assert decoded.required_id == 42
    # Explicit defaults
    assert decoded.name == "unknown"
    assert decoded.port == 8080
    assert decoded.enabled == true
    assert decoded.status == :ACTIVE
    # Optional field without default is nil (not present)
    assert decoded.optional_field == nil
    assert decoded.optional_id == nil
  end

  test "encodes and decodes explicitly set values overriding defaults", %{schema: schema} do
    request_msg = Schema.message(schema, "Proto2Request")

    data = %{
      required_field: "test",
      required_id: 1,
      name: "custom_server",
      port: 9000,
      enabled: false,
      status: :INACTIVE,
      optional_field: "present",
      optional_id: 100,
      extended_field: "extended_value"
    }

    assert {:ok, encoded} = Protobuf.encode(request_msg, data, schema)
    assert {:ok, decoded} = Protobuf.decode(request_msg, encoded, schema)

    assert decoded.required_field == "test"
    assert decoded.required_id == 1
    assert decoded.name == "custom_server"
    assert decoded.port == 9000
    assert decoded.enabled == false
    assert decoded.status == :INACTIVE
    assert decoded.optional_field == "present"
    assert decoded.optional_id == 100
    assert decoded.extended_field == "extended_value"
  end

  test "enforces required fields on encode", %{schema: schema} do
    request_msg = Schema.message(schema, "Proto2Request")

    # Missing required_id
    data = %{required_field: "only_one"}
    assert {:error, _code, msg} = Protobuf.encode(request_msg, data, schema)
    assert msg =~ "missing required field" or msg =~ "internal error"

    assert_raise MatchError, fn ->
      {:ok, _} = Protobuf.encode(request_msg, data, schema)
    end
  end

  test "enforces required fields on decode", %{schema: schema} do
    response_msg = Schema.message(schema, "Proto2Response")

    # Empty payload lacks required bool success = 1;
    empty_payload = <<>>

    assert {:error, 3, msg} = Protobuf.decode(response_msg, empty_payload, schema)
    assert msg =~ "missing required field 'success'"
  end

  test "encodes packed repeated field in proto2", %{schema: schema} do
    request_msg = Schema.message(schema, "Proto2Request")

    data = %{
      required_field: "packed_test",
      required_id: 99,
      packed_ints: [10, 20, 30]
    }

    assert {:ok, encoded} = Protobuf.encode(request_msg, data, schema)
    assert {:ok, decoded} = Protobuf.decode(request_msg, encoded, schema)

    assert decoded.packed_ints == [10, 20, 30]
  end

  test "generates correct reflection FileDescriptorProto for proto2 schema", %{schema: schema} do
    assert {:ok, fdp_bytes} = {:ok, Descriptor.encode_file_descriptor(schema)}
    assert is_binary(fdp_bytes)

    # Decode back into schema
    decoded_schema = Descriptor.decode_file_descriptor(fdp_bytes)
    assert decoded_schema.syntax == :proto2
    assert decoded_schema.package == "proto2_features"

    request_msg = Schema.message(decoded_schema, "Proto2Request")
    assert request_msg != nil

    req_field = Schema.Message.field_by_name(request_msg, :required_field)
    assert req_field != nil
    assert req_field.prefix == :required
  end
end
