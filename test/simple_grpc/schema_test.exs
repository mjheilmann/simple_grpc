defmodule SimpleGrpc.SchemaTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.{Service, Message, RPC, Field}

  describe "from_file/1" do
    test "loads schema from custom_prefix_service.proto" do
      {:ok, schema} = Schema.from_file("priv/protos/custom_prefix/custom_prefix_service.proto")

      assert schema.package == "custom_prefix"
      assert schema.syntax == :proto3

      # Service checks
      service = Schema.service(schema, "PrefixService")
      assert %Service{} = service
      assert service.full_name == "custom_prefix.PrefixService"
      assert service.path_prefix == "/custom_prefix.PrefixService"

      # RPC checks
      assert Map.has_key?(service.rpcs, "Echo")
      rpc = service.rpcs["Echo"]
      assert %RPC{name: "Echo", input: "EchoRequest", output: "EchoResponse"} = rpc
      assert rpc.path == "/custom_prefix.PrefixService/Echo"

      # Message checks
      req_msg = Schema.message(schema, "EchoRequest")
      assert %Message{name: "EchoRequest"} = req_msg

      assert %Field{name: :message, number: 1, type: "string"} =
               Message.field_by_number(req_msg, 1)

      assert %Field{name: :message, number: 1} = Message.field_by_name(req_msg, :message)
    end

    test "loads schema with streaming rpcs" do
      {:ok, schema} = Schema.from_file("priv/protos/streaming/streaming_service.proto")
      service = Schema.service(schema, "StreamingService")

      assert service.rpcs["UnaryCall"].request_stream == false
      assert service.rpcs["UnaryCall"].response_stream == false

      assert service.rpcs["ServerStreamingCall"].request_stream == false
      assert service.rpcs["ServerStreamingCall"].response_stream == true

      assert service.rpcs["ClientStreamingCall"].request_stream == true
      assert service.rpcs["ClientStreamingCall"].response_stream == false

      assert service.rpcs["BidirectionalStreamingCall"].request_stream == true
      assert service.rpcs["BidirectionalStreamingCall"].response_stream == true
    end

    test "loads schema with nested messages and enums" do
      {:ok, schema} = Schema.from_file("priv/protos/nested/nested_messages.proto")
      outer_msg = Schema.message(schema, "OuterMessage")
      assert length(outer_msg.nested_messages) > 0

      complex_msg = Schema.message(schema, "ComplexNested")
      assert length(complex_msg.nested_enums) > 0
    end

    test "find_rpc finds RPC by exact gRPC path" do
      {:ok, schema} = Schema.from_file("priv/protos/custom_prefix/custom_prefix_service.proto")
      {service, rpc} = Schema.find_rpc(schema, "/custom_prefix.PrefixService/Echo")

      assert service.name == "PrefixService"
      assert rpc.name == "Echo"
    end

    test "loads schema and automatically follows imports (cross-file & Google WKT)" do
      {:ok, schema} = Schema.from_file("priv/protos/imports_test/imports_test.proto")

      assert schema.package == "imports_test"

      # Primary service
      service = Schema.service(schema, "ImportTestService")
      assert %Service{} = service

      # Imported message from common/common_types.proto
      address_msg = Schema.message(schema, "common.Address")
      assert %Message{name: "Address", full_name: "common.Address"} = address_msg
      assert Message.field_by_name(address_msg, :street).type == "string"

      # Unqualified lookup also resolves
      assert %Message{name: "Address"} = Schema.message(schema, "Address")

      # Imported enum from common/common_types.proto
      priority_enum = Schema.enum(schema, "common.Priority")
      assert priority_enum.fields[:HIGH] == 3

      # Imported Google WKT Timestamp
      timestamp_msg = Schema.message(schema, "google.protobuf.Timestamp")
      assert %Message{name: "Timestamp", full_name: "google.protobuf.Timestamp"} = timestamp_msg
      assert Message.field_by_name(timestamp_msg, :seconds).type == "int64"
    end

    test "loads all standard Google Well-Known Types" do
      {:ok, schema} = Schema.from_file("priv/protos/well_known_types/well_known_types.proto")

      # Timestamp
      assert %Message{name: "Timestamp"} = Schema.message(schema, "google.protobuf.Timestamp")
      # Duration
      assert %Message{name: "Duration"} = Schema.message(schema, "google.protobuf.Duration")
      # Any
      assert %Message{name: "Any"} = Schema.message(schema, "google.protobuf.Any")
      # Empty
      assert %Message{name: "Empty"} = Schema.message(schema, "google.protobuf.Empty")
      # Struct & Value
      assert %Message{name: "Struct"} = Schema.message(schema, "google.protobuf.Struct")
      assert %Message{name: "Value"} = Schema.message(schema, "google.protobuf.Value")
      assert %Message{name: "ListValue"} = Schema.message(schema, "google.protobuf.ListValue")
      # Wrappers
      assert %Message{name: "StringValue"} = Schema.message(schema, "google.protobuf.StringValue")
      assert %Message{name: "Int32Value"} = Schema.message(schema, "google.protobuf.Int32Value")
      assert %Message{name: "BoolValue"} = Schema.message(schema, "google.protobuf.BoolValue")
      # FieldMask
      assert %Message{name: "FieldMask"} = Schema.message(schema, "google.protobuf.FieldMask")
    end
  end

  describe "from_files/2 and merge/1" do
    test "loads multiple proto files and merges them into a unified schema" do
      files = [
        "priv/protos/common/common_types.proto",
        "priv/protos/custom_prefix/custom_prefix_service.proto"
      ]

      {:ok, schema} = Schema.from_files(files)

      assert Map.has_key?(schema.services, "PrefixService")
      assert Map.has_key?(schema.messages, "EchoRequest")
      assert Map.has_key?(schema.messages, "common.Address")
      assert Map.has_key?(schema.enums, "common.Priority")
    end

    test "Schema.merge/1 merges schemas correctly" do
      {:ok, s1} = Schema.from_file("priv/protos/common/common_types.proto")
      {:ok, s2} = Schema.from_file("priv/protos/custom_prefix/custom_prefix_service.proto")

      merged = Schema.merge([s1, s2])

      assert Schema.service(merged, "PrefixService") != nil
      assert Schema.message(merged, "Address") != nil
      assert Schema.enum(merged, "Priority") != nil
    end
  end

  describe "from_directory/2" do
    test "loads all proto files in a directory recursively" do
      {:ok, schema} = Schema.from_directory("priv/protos/nested")

      assert Schema.message(schema, "OuterMessage") != nil
      assert Schema.message(schema, "ComplexNested") != nil
    end

    test "loads from directory and prunes schema to only the needful service and its dependencies" do
      {:ok, schema} = Schema.from_directory("priv/protos/imports_test", service: "ImportTestService")

      # Retains only the target service
      assert Map.has_key?(schema.services, "ImportTestService")
      assert not Map.has_key?(schema.services, "PrefixService")
      assert not Map.has_key?(schema.services, "StreamingService")

      # Retains direct and transitive messages
      assert Map.has_key?(schema.messages, "UserRequest")
      assert Map.has_key?(schema.messages, "UserResponse")
      assert Map.has_key?(schema.messages, "common.Address")
      assert Map.has_key?(schema.messages, "common.Coordinates")
      assert Map.has_key?(schema.messages, "google.protobuf.Timestamp")

      # Retains referenced enums
      assert Map.has_key?(schema.enums, "common.Priority")

      # Does not retain unrelated messages from other services
      assert not Map.has_key?(schema.messages, "EchoRequest")
      assert not Map.has_key?(schema.messages, "ComplicatedRequest")
    end
  end

  describe "filter_for_service/2" do
    test "returns error if service not found" do
      {:ok, schema} = Schema.from_file("priv/protos/custom_prefix/custom_prefix_service.proto")
      assert {:error, msg} = Schema.filter_for_service(schema, "NonExistentService")
      assert msg =~ "not found"
    end
  end

  describe "Cross-File Protobuf Binary Encoding and Decoding" do
    test "encodes and decodes messages with cross-file imported submessages, enums, and timestamps" do
      {:ok, schema} = Schema.from_file("priv/protos/imports_test/imports_test.proto")
      req_msg = Schema.message(schema, "UserRequest")

      user_payload = %{
        name: "Alice",
        email: "alice@example.com",
        address: %{
          street: "123 Main St",
          city: "Wonderland",
          state: "CA",
          postal_code: "90210",
          country: "USA"
        },
        location: %{
          latitude: 37.7749,
          longitude: -122.4194,
          altitude: 15.0
        },
        registered_at: %{
          seconds: 1_700_000_000,
          nanos: 500_000
        }
      }

      encoded = SimpleGrpc.Protobuf.encode!(req_msg, user_payload, schema)
      assert byte_size(encoded) > 0

      decoded = SimpleGrpc.Protobuf.decode!(req_msg, encoded, schema)
      assert decoded.name == "Alice"
      assert decoded.email == "alice@example.com"
      assert decoded.address.street == "123 Main St"
      assert decoded.address.city == "Wonderland"
      assert_in_delta decoded.location.latitude, 37.7749, 0.0001
      assert decoded.registered_at.seconds == 1_700_000_000
      assert decoded.registered_at.nanos == 500_000
    end

    test "encodes and decodes imported enums across files" do
      {:ok, schema} = Schema.from_file("priv/protos/imports_test/imports_test.proto")
      resp_msg = Schema.message(schema, "UserResponse")

      payload = %{
        user_id: "user_42",
        priority: :HIGH,
        created_at: %{seconds: 1_600_000_000, nanos: 0}
      }

      encoded = SimpleGrpc.Protobuf.encode!(resp_msg, payload, schema)
      decoded = SimpleGrpc.Protobuf.decode!(resp_msg, encoded, schema)

      assert decoded.user_id == "user_42"
      assert decoded.priority == :HIGH
      assert decoded.created_at.seconds == 1_600_000_000
    end
  end

  describe "from_string/1" do
    test "parses schema from raw protobuf string" do
      proto = """
      syntax = "proto3";
      package test_pkg;

      service Greeter {
        rpc SayHello (HelloRequest) returns (HelloReply);
      }

      message HelloRequest {
        string name = 1;
        int32 age = 2;
      }

      message HelloReply {
        string message = 1;
      }
      """

      {:ok, schema} = Schema.from_string(proto)

      assert schema.package == "test_pkg"
      assert Map.has_key?(schema.services, "Greeter")
      assert Map.has_key?(schema.messages, "HelloRequest")

      req_msg = schema.messages["HelloRequest"]
      assert Message.field_by_number(req_msg, 2).name == :age
      assert Message.field_by_number(req_msg, 2).type == "int32"
    end
  end
end
