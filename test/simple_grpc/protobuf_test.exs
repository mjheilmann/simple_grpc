defmodule SimpleGrpc.ProtobufTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.{Message, Field}

  describe "Varint & ZigZag utilities" do
    test "encodes and decodes 1-byte varints" do
      assert Protobuf.encode_varint(0) == <<0>>
      assert Protobuf.encode_varint(1) == <<1>>
      assert Protobuf.encode_varint(127) == <<127>>

      assert {0, <<>>} = Protobuf.decode_varint(<<0>>)
      assert {1, <<>>} = Protobuf.decode_varint(<<1>>)
      assert {127, <<>>} = Protobuf.decode_varint(<<127>>)
    end

    test "encodes and decodes multi-byte varints" do
      # 300 = 0b100101100 -> [0xAC, 0x02]
      encoded = Protobuf.encode_varint(300)
      assert encoded == <<0xAC, 0x02>>
      assert {300, <<>>} = Protobuf.decode_varint(encoded)

      large = 123_456_789
      encoded_large = Protobuf.encode_varint(large)
      assert {^large, <<>>} = Protobuf.decode_varint(encoded_large)
    end

    test "ZigZag 32-bit conversion" do
      assert Protobuf.encode_zigzag32(0) == 0
      assert Protobuf.encode_zigzag32(-1) == 1
      assert Protobuf.encode_zigzag32(1) == 2
      assert Protobuf.encode_zigzag32(-2) == 3
      assert Protobuf.encode_zigzag32(2) == 4

      assert Protobuf.decode_zigzag32(0) == 0
      assert Protobuf.decode_zigzag32(1) == -1
      assert Protobuf.decode_zigzag32(2) == 1
      assert Protobuf.decode_zigzag32(3) == -2
    end
  end

  describe "Encoding and Decoding using Message Descriptors" do
    setup do
      schema = Schema.from_file!("priv/protos/custom_prefix/custom_prefix_service.proto")
      req_msg = Schema.message(schema, "EchoRequest")
      resp_msg = Schema.message(schema, "EchoResponse")
      %{schema: schema, req_msg: req_msg, resp_msg: resp_msg}
    end

    test "encodes and decodes string fields", %{req_msg: req_msg} do
      payload = %{message: "Hello World"}
      encoded = Protobuf.encode!(req_msg, payload)

      # Tag: field 1, wire type 2 -> (1 <<< 3) | 2 = 10 (0x0A)
      # Length: 11 (0x0B)
      assert <<0x0A, 11, "Hello World">> = encoded

      decoded = Protobuf.decode!(req_msg, encoded)
      assert decoded.message == "Hello World"
    end

    test "omits proto3 default values when encoding", %{req_msg: req_msg} do
      payload = %{message: ""}
      encoded = Protobuf.encode!(req_msg, payload)
      assert encoded == <<>>

      decoded = Protobuf.decode!(req_msg, encoded)
      assert decoded.message == ""
    end

    test "encodes and decodes multiple scalar types" do
      msg = %Message{
        name: "Scalars",
        fields: [
          %Field{name: :int_val, number: 1, type: "int32", kind: :field},
          %Field{name: :sint_val, number: 2, type: "sint32", kind: :field},
          %Field{name: :bool_val, number: 3, type: "bool", kind: :field},
          %Field{name: :float_val, number: 4, type: "float", kind: :field},
          %Field{name: :double_val, number: 5, type: "double", kind: :field},
          %Field{name: :str_val, number: 6, type: "string", kind: :field},
          %Field{name: :bytes_val, number: 7, type: "bytes", kind: :field}
        ],
        fields_by_number: %{
          1 => %Field{name: :int_val, number: 1, type: "int32", kind: :field},
          2 => %Field{name: :sint_val, number: 2, type: "sint32", kind: :field},
          3 => %Field{name: :bool_val, number: 3, type: "bool", kind: :field},
          4 => %Field{name: :float_val, number: 4, type: "float", kind: :field},
          5 => %Field{name: :double_val, number: 5, type: "double", kind: :field},
          6 => %Field{name: :str_val, number: 6, type: "string", kind: :field},
          7 => %Field{name: :bytes_val, number: 7, type: "bytes", kind: :field}
        }
      }

      data = %{
        int_val: 42,
        sint_val: -10,
        bool_val: true,
        float_val: 3.140000104904175,
        double_val: 2.718281828459045,
        str_val: "Elixir gRPC",
        bytes_val: <<1, 2, 3, 4>>
      }

      encoded = Protobuf.encode!(msg, data)
      decoded = Protobuf.decode!(msg, encoded)

      assert decoded.int_val == 42
      assert decoded.sint_val == -10
      assert decoded.bool_val == true
      assert_in_delta decoded.float_val, 3.14, 0.001
      assert_in_delta decoded.double_val, 2.71828, 0.00001
      assert decoded.str_val == "Elixir gRPC"
      assert decoded.bytes_val == <<1, 2, 3, 4>>
    end

    test "encodes and decodes repeated packed numbers" do
      msg = %Message{
        name: "RepeatedMessage",
        fields: [
          %Field{name: :numbers, number: 1, type: "int32", prefix: :repeated, kind: :field}
        ],
        fields_by_number: %{
          1 => %Field{name: :numbers, number: 1, type: "int32", prefix: :repeated, kind: :field}
        }
      }

      data = %{numbers: [1, 2, 3, 4, 5]}
      encoded = Protobuf.encode!(msg, data)
      decoded = Protobuf.decode!(msg, encoded)

      assert decoded.numbers == [1, 2, 3, 4, 5]
    end

    test "encodes and decodes repeated strings (unpacked)" do
      msg = %Message{
        name: "RepeatedStrings",
        fields: [
          %Field{name: :tags, number: 1, type: "string", prefix: :repeated, kind: :field}
        ],
        fields_by_number: %{
          1 => %Field{name: :tags, number: 1, type: "string", prefix: :repeated, kind: :field}
        }
      }

      data = %{tags: ["alpha", "beta", "gamma"]}
      encoded = Protobuf.encode!(msg, data)
      decoded = Protobuf.decode!(msg, encoded)

      assert decoded.tags == ["alpha", "beta", "gamma"]
    end

    test "encodes and decodes map fields" do
      msg = %Message{
        name: "MapMessage",
        fields: [
          %Field{name: :attributes, number: 1, key_type: "string", val_type: "string", kind: :map}
        ],
        fields_by_number: %{
          1 => %Field{name: :attributes, number: 1, key_type: "string", val_type: "string", kind: :map}
        }
      }

      data = %{attributes: %{"env" => "prod", "version" => "1.0"}}
      encoded = Protobuf.encode!(msg, data)
      decoded = Protobuf.decode!(msg, encoded)

      assert decoded.attributes == %{"env" => "prod", "version" => "1.0"}
    end

    test "encodes and decodes oneof fields" do
      msg = %Message{
        name: "OneofMessage",
        fields: [
          %Field{
            name: :result,
            kind: :oneof,
            fields: [
              %Field{name: :success_msg, number: 1, type: "string", kind: :field},
              %Field{name: :error_code, number: 2, type: "int32", kind: :field}
            ]
          }
        ],
        fields_by_number: %{
          1 => %Field{name: :success_msg, number: 1, type: "string", kind: :field},
          2 => %Field{name: :error_code, number: 2, type: "int32", kind: :field}
        }
      }

      data1 = %{success_msg: "OK"}
      encoded1 = Protobuf.encode!(msg, data1)
      decoded1 = Protobuf.decode!(msg, encoded1)
      assert decoded1.success_msg == "OK"
      assert is_nil(decoded1[:error_code])

      data2 = %{error_code: 404}
      encoded2 = Protobuf.encode!(msg, data2)
      decoded2 = Protobuf.decode!(msg, encoded2)
      assert decoded2.error_code == 404
      assert is_nil(decoded2[:success_msg])
    end

    test "gracefully skips unknown wire fields during decode" do
      # Message on wire has fields 1 and 2
      wire_msg = %Message{
        name: "WireMsg",
        fields: [
          %Field{name: :f1, number: 1, type: "string", kind: :field},
          %Field{name: :f2, number: 2, type: "int32", kind: :field}
        ],
        fields_by_number: %{
          1 => %Field{name: :f1, number: 1, type: "string", kind: :field},
          2 => %Field{name: :f2, number: 2, type: "int32", kind: :field}
        }
      }

      encoded = Protobuf.encode!(wire_msg, %{f1: "keep me", f2: 999})

      # Receiver schema only knows about field 1
      receiver_msg = %Message{
        name: "ReceiverMsg",
        fields: [
          %Field{name: :f1, number: 1, type: "string", kind: :field}
        ],
        fields_by_number: %{
          1 => %Field{name: :f1, number: 1, type: "string", kind: :field}
        }
      }

      decoded = Protobuf.decode!(receiver_msg, encoded)
      assert decoded.f1 == "keep me"
      assert is_nil(decoded[:f2])
    end
  end
end
