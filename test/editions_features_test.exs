defmodule SimpleGrpc.EditionsFeaturesTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Reflection.Descriptor

  describe "Editions schema and feature flag resolution" do
    test "parses schema with edition 2023 and defaults" do
      proto = """
      edition = "2023";
      package test.editions;

      message User {
        string id = 1;
        int32 score = 2;
        repeated int32 tags = 3;
      }
      """

      assert {:ok, schema} = Schema.from_string(proto)
      assert schema.syntax == :editions
      assert schema.edition == "2023"
      assert schema.features.field_presence == :explicit
      assert schema.features.repeated_field_encoding == :packed

      user_msg = Schema.message(schema, "test.editions.User")
      assert user_msg != nil

      id_field = Schema.Message.field_by_name(user_msg, :id)
      score_field = Schema.Message.field_by_name(user_msg, :score)
      tags_field = Schema.Message.field_by_name(user_msg, :tags)

      # In Edition 2023, default field presence is EXPLICIT (tracked as :optional)
      assert id_field.prefix == :optional
      assert score_field.prefix == :optional
      assert tags_field.prefix == :repeated
      assert tags_field.is_packable == true
    end

    test "features.field_presence = LEGACY_REQUIRED enforces presence on encode and decode" do
      proto = """
      edition = "2023";
      package test.editions;

      message StrictAccount {
        string account_id = 1 [features.field_presence = LEGACY_REQUIRED];
        string nickname = 2;
      }
      """

      assert {:ok, schema} = Schema.from_string(proto)
      account_msg = Schema.message(schema, "test.editions.StrictAccount")
      assert account_msg.required_fields == [:account_id]

      # Encode with missing required field throws error
      assert {:error, _code, msg} = Protobuf.encode(account_msg, %{nickname: "Tester"}, schema)
      assert msg =~ "missing required field" or msg =~ "internal error"

      # Encode with valid account_id succeeds
      assert {:ok, encoded} =
               Protobuf.encode(account_msg, %{account_id: "acc-123", nickname: "Tester"}, schema)

      assert {:ok, decoded} = Protobuf.decode(account_msg, encoded, schema)
      assert decoded.account_id == "acc-123"
      assert decoded.nickname == "Tester"

      # Decode with empty payload fails required check
      assert {:error, _code, msg2} = Protobuf.decode(account_msg, <<>>, schema)
      assert msg2 =~ "missing required field 'account_id'"
    end

    test "features.field_presence = IMPLICIT behaves like proto3" do
      proto = """
      edition = "2023";
      package test.editions;

      message ImplicitConfig {
        option features.field_presence = IMPLICIT;

        int32 max_connections = 1;
        string hostname = 2;
      }
      """

      assert {:ok, schema} = Schema.from_string(proto)
      cfg_msg = Schema.message(schema, "test.editions.ImplicitConfig")

      conn_field = Schema.Message.field_by_name(cfg_msg, :max_connections)
      assert conn_field.prefix == :implicit

      # Zero values are omitted in IMPLICIT presence
      assert {:ok, encoded} =
               Protobuf.encode(cfg_msg, %{max_connections: 0, hostname: ""}, schema)

      assert byte_size(encoded) == 0
    end

    test "features.repeated_field_encoding = EXPANDED produces unpacked wire format" do
      proto = """
      edition = "2023";
      package test.editions;

      message UnpackedData {
        repeated int32 numbers = 1 [features.repeated_field_encoding = EXPANDED];
      }
      """

      assert {:ok, schema} = Schema.from_string(proto)
      data_msg = Schema.message(schema, "test.editions.UnpackedData")
      num_field = Schema.Message.field_by_name(data_msg, :numbers)
      assert num_field.is_packable == false

      # Wire format for unpacked: tag-per-element (field 1, wire_type 0 = 0x08)
      assert {:ok, encoded} = Protobuf.encode(data_msg, %{numbers: [10, 20]}, schema)
      assert encoded == <<0x08, 10, 0x08, 20>>

      assert {:ok, decoded} = Protobuf.decode(data_msg, encoded, schema)
      assert decoded.numbers == [10, 20]
    end

    test "features inherit properly from file to message to field" do
      proto = """
      edition = "2023";
      package test.editions;

      option features.field_presence = IMPLICIT;

      message ParentConfig {
        int32 file_inherited = 1;
        int32 explicit_override = 2 [features.field_presence = EXPLICIT];

        message NestedChild {
          option features.field_presence = LEGACY_REQUIRED;
          string token = 1;
        }
      }
      """

      assert {:ok, schema} = Schema.from_string(proto)
      parent = Schema.message(schema, "test.editions.ParentConfig")
      child = Schema.message(schema, "test.editions.ParentConfig.NestedChild")

      assert parent.features.field_presence == :implicit
      assert Schema.Message.field_by_name(parent, :file_inherited).prefix == :implicit
      assert Schema.Message.field_by_name(parent, :explicit_override).prefix == :optional

      assert child.features.field_presence == :legacy_required
      assert Schema.Message.field_by_name(child, :token).prefix == :required
    end

    test "reflection exposes edition and features" do
      proto = """
      edition = "2023";
      package test.editions;

      message SampleMsg {
        string text = 1;
      }
      """

      assert {:ok, schema} = Schema.from_string(proto)
      fdp_bytes = Descriptor.encode_file_descriptor(schema)
      assert is_binary(fdp_bytes)

      reconstructed = Descriptor.decode_file_descriptor(fdp_bytes)
      assert reconstructed.syntax == :editions
      assert reconstructed.messages["test.editions.SampleMsg"] != nil
    end
  end
end
