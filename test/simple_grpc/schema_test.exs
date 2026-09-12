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
