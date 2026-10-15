defmodule SimpleGrpc.ReflectionTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.{Registry, Schema, Protobuf}
  alias SimpleGrpc.Reflection.{Descriptor, Server}

  setup do
    Registry.clear()
    :ok
  end

  describe "Descriptor.encode_file_descriptor/1" do
    test "encodes schema into valid FileDescriptorProto binary payload" do
      {:ok, schema} = Schema.from_file("test/fixtures/protos/streaming/streaming_service.proto")
      fdp_bytes = Descriptor.encode_file_descriptor(schema)

      assert is_binary(fdp_bytes)
      assert byte_size(fdp_bytes) > 0

      # Decode back using descriptor schema to verify fields
      decoded =
        Protobuf.decode!(
          Descriptor.__info__(:functions)
          |> List.first()
          |> then(fn _ ->
            # Decode using descriptor schema
            schema_proto = %SimpleGrpc.Schema.Message{
              name: "FileDescriptorProto",
              fields: [
                %SimpleGrpc.Schema.Field{name: :name, number: 1, type: "string", kind: :field},
                %SimpleGrpc.Schema.Field{name: :package, number: 2, type: "string", kind: :field},
                %SimpleGrpc.Schema.Field{name: :syntax, number: 12, type: "string", kind: :field}
              ],
              fields_by_number: %{
                1 => %SimpleGrpc.Schema.Field{
                  name: :name,
                  number: 1,
                  type: "string",
                  kind: :field
                },
                2 => %SimpleGrpc.Schema.Field{
                  name: :package,
                  number: 2,
                  type: "string",
                  kind: :field
                },
                12 => %SimpleGrpc.Schema.Field{
                  name: :syntax,
                  number: 12,
                  type: "string",
                  kind: :field
                }
              }
            }

            schema_proto
          end),
          fdp_bytes
        )

      assert decoded.package == "streaming"
      assert decoded.syntax == "proto3"
    end
  end

  describe "Server.server_reflection_info/1" do
    setup do
      {:ok, schema} = Schema.from_file("test/fixtures/protos/streaming/streaming_service.proto")

      Registry.register_service(schema, %{
        unary_call: fn req -> {:ok, req} end
      })

      Server.register()
      %{schema: schema}
    end

    test "handles list_services query" do
      req = %{list_services: ""}
      assert {:ok, resp} = Server.server_reflection_info(req)

      service_names = Enum.map(resp.list_services_response.service, & &1.name)

      assert "streaming.StreamingService" in service_names
      assert "grpc.reflection.v1.ServerReflection" in service_names
      assert "grpc.reflection.v1alpha.ServerReflection" in service_names
    end

    test "handles file_containing_symbol query for service" do
      req = %{file_containing_symbol: "streaming.StreamingService"}
      assert {:ok, resp} = Server.server_reflection_info(req)

      assert %{file_descriptor_proto: protos} = resp.file_descriptor_response
      assert length(protos) > 0
    end

    test "handles file_containing_symbol query for message and method" do
      # Message
      req_msg = %{file_containing_symbol: "streaming.StreamRequest"}
      assert {:ok, resp_msg} = Server.server_reflection_info(req_msg)
      assert is_map(resp_msg.file_descriptor_response)

      # Method
      req_method = %{file_containing_symbol: "streaming.StreamingService.UnaryCall"}
      assert {:ok, resp_method} = Server.server_reflection_info(req_method)
      assert is_map(resp_method.file_descriptor_response)
    end

    test "returns error response when symbol not found" do
      req = %{file_containing_symbol: "nonexistent.Symbol"}
      assert {:ok, resp} = Server.server_reflection_info(req)

      assert %{error_code: 5, error_message: msg} = resp.error_response
      assert msg =~ "not found"
    end

    test "handles all_extension_numbers_of_type query" do
      req = %{all_extension_numbers_of_type: "streaming.StreamRequest"}
      assert {:ok, resp} = Server.server_reflection_info(req)

      assert resp.all_extension_numbers_response.base_type_name == "streaming.StreamRequest"
      assert resp.all_extension_numbers_response.extension_number == []
    end

    test "respects only: filter option" do
      req_list = %{list_services: ""}

      assert {:ok, resp} =
               Server.server_reflection_info(req_list, nil, only: ["streaming.StreamingService"])

      service_names = Enum.map(resp.list_services_response.service, & &1.name)

      assert "streaming.StreamingService" in service_names
      refute "streaming.MultiStreamService" in service_names
    end

    test "respects except: filter option" do
      req_list = %{list_services: ""}

      assert {:ok, resp} =
               Server.server_reflection_info(req_list, nil,
                 except: ["streaming.MultiStreamService"]
               )

      service_names = Enum.map(resp.list_services_response.service, & &1.name)

      assert "streaming.StreamingService" in service_names
      refute "streaming.MultiStreamService" in service_names
    end
  end
end
