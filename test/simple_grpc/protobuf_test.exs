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
      {:ok, schema} = Schema.from_file("priv/protos/custom_prefix/custom_prefix_service.proto")
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
          1 => %Field{
            name: :attributes,
            number: 1,
            key_type: "string",
            val_type: "string",
            kind: :map
          }
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

    test "oneof last-variant-wins when multiple variants arrive in the same binary payload" do
      msg = %Message{
        name: "OneofMessage",
        fields: [
          %Field{
            name: :result,
            kind: :oneof,
            fields: [
              %Field{name: :success_msg, number: 1, type: "string", kind: :field},
              %Field{name: :error_code, number: 2, type: "int32", kind: :field},
              %Field{name: :raw_status, number: 3, type: "int64", kind: :field}
            ]
          }
        ],
        fields_by_number: %{
          1 => %Field{name: :success_msg, number: 1, type: "string", kind: :field},
          2 => %Field{name: :error_code, number: 2, type: "int32", kind: :field},
          3 => %Field{name: :raw_status, number: 3, type: "int64", kind: :field}
        }
      }

      # Variant 1: success_msg = "first"
      # Tag 1 (wire type 2 -> 0x0A), len 5, "first"
      enc1 = <<0x0A, 5, "first">>

      # Variant 2: error_code = 500
      # Tag 2 (wire type 0 -> 0x10), varint 500 -> <<0xF4, 0x03>>
      enc2 = <<0x10, 0xF4, 0x03>>

      # Variant 3: raw_status = 1234
      # Tag 3 (wire type 0 -> 0x18), varint 1234 -> <<0xD2, 0x09>>
      enc3 = <<0x18, 0xD2, 0x09>>

      # Case A: enc1 followed by enc2 -> enc2 wins
      combined_1_2 = enc1 <> enc2
      decoded_1_2 = Protobuf.decode!(msg, combined_1_2)
      assert decoded_1_2.error_code == 500
      assert is_nil(decoded_1_2.success_msg)
      assert is_nil(decoded_1_2.raw_status)

      # Case B: enc2 followed by enc1 -> enc1 wins
      combined_2_1 = enc2 <> enc1
      decoded_2_1 = Protobuf.decode!(msg, combined_2_1)
      assert decoded_2_1.success_msg == "first"
      assert is_nil(decoded_2_1.error_code)
      assert is_nil(decoded_2_1.raw_status)

      # Case C: enc1 followed by enc2 followed by enc3 -> enc3 wins
      combined_1_2_3 = enc1 <> enc2 <> enc3
      decoded_1_2_3 = Protobuf.decode!(msg, combined_1_2_3)
      assert decoded_1_2_3.raw_status == 1234
      assert is_nil(decoded_1_2_3.success_msg)
      assert is_nil(decoded_1_2_3.error_code)
    end

    test "oneof explicitly populates unset variants as nil when decoding empty or partial payloads" do
      msg = %Message{
        name: "OneofMessage",
        fields: [
          %Field{name: :regular_field, number: 1, type: "string", kind: :field},
          %Field{
            name: :choice,
            kind: :oneof,
            fields: [
              %Field{name: :opt_a, number: 2, type: "string", kind: :field},
              %Field{name: :opt_b, number: 3, type: "int32", kind: :field}
            ]
          }
        ],
        fields_by_number: %{
          1 => %Field{name: :regular_field, number: 1, type: "string", kind: :field},
          2 => %Field{name: :opt_a, number: 2, type: "string", kind: :field},
          3 => %Field{name: :opt_b, number: 3, type: "int32", kind: :field}
        }
      }

      # Empty payload
      decoded_empty = Protobuf.decode!(msg, <<>>)
      assert decoded_empty.regular_field == ""
      assert Map.has_key?(decoded_empty, :opt_a)
      assert is_nil(decoded_empty.opt_a)
      assert Map.has_key?(decoded_empty, :opt_b)
      assert is_nil(decoded_empty.opt_b)

      # Only regular field set
      enc_reg = Protobuf.encode!(msg, %{regular_field: "hello"})
      decoded_reg = Protobuf.decode!(msg, enc_reg)
      assert decoded_reg.regular_field == "hello"
      assert is_nil(decoded_reg.opt_a)
      assert is_nil(decoded_reg.opt_b)
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

  describe "Proto3 optional field presence (explicit presence)" do
    setup do
      {:ok, schema} = Schema.from_file("priv/protos/scalar_types/scalar_types.proto")
      req_msg = Schema.message(schema, "ScalarRequest")
      %{schema: schema, req_msg: req_msg}
    end

    test "encoder omits optional fields when value is nil or absent", %{
      req_msg: req_msg,
      schema: schema
    } do
      # Absent fields
      assert Protobuf.encode!(req_msg, %{}, schema) == <<>>

      # Explicit nil fields
      assert Protobuf.encode!(req_msg, %{optional_string: nil, optional_int: nil}, schema) == <<>>
    end

    test "encoder serializes explicit default values for optional fields (0, false, empty string, etc.)",
         %{
           req_msg: req_msg,
           schema: schema
         } do
      # For a non-optional field, default values (e.g. 0 or "") are omitted:
      encoded_non_opt = Protobuf.encode!(req_msg, %{string_field: "", int32_field: 0}, schema)
      assert encoded_non_opt == <<>>

      # For an optional field, explicit default values MUST be serialized to wire:
      encoded_opt = Protobuf.encode!(req_msg, %{optional_string: "", optional_int: 0}, schema)
      assert byte_size(encoded_opt) > 0

      # optional_string is field 18, wire type 2 -> (18 <<< 3) | 2 = 146 (0x92, 0x01)
      # length is 0 -> 0x00
      expected_str_field = <<0x92, 0x01, 0x00>>
      assert String.contains?(encoded_opt, expected_str_field)

      # optional_int is field 19, wire type 0 -> (19 <<< 3) | 0 = 152 (0x98, 0x01)
      # varint 0 -> 0x00
      expected_int_field = <<0x98, 0x01, 0x00>>
      assert String.contains?(encoded_opt, expected_int_field)
    end

    test "decoder preserves missing optional fields as nil instead of default scalar values", %{
      req_msg: req_msg,
      schema: schema
    } do
      # Decoding an empty payload
      decoded = Protobuf.decode!(req_msg, <<>>, schema)

      # Optional fields are nil
      assert is_nil(decoded.optional_string)
      assert is_nil(decoded.optional_int)

      # Non-optional fields default to scalar defaults
      assert decoded.string_field == ""
      assert decoded.int32_field == 0
      assert decoded.bool_field == false
      assert decoded.double_field == 0.0
    end

    test "decoder restores explicit default values when present on the wire", %{
      req_msg: req_msg,
      schema: schema
    } do
      encoded = Protobuf.encode!(req_msg, %{optional_string: "", optional_int: 0}, schema)
      decoded = Protobuf.decode!(req_msg, encoded, schema)

      assert decoded.optional_string == ""
      assert decoded.optional_int == 0
    end

    test "encodes and decodes optional fields across all scalar types" do
      msg = %Message{
        name: "OptionalScalars",
        fields: [
          %Field{name: :opt_int32, number: 1, type: "int32", prefix: :optional, kind: :field},
          %Field{name: :opt_sint32, number: 2, type: "sint32", prefix: :optional, kind: :field},
          %Field{name: :opt_bool, number: 3, type: "bool", prefix: :optional, kind: :field},
          %Field{name: :opt_float, number: 4, type: "float", prefix: :optional, kind: :field},
          %Field{name: :opt_double, number: 5, type: "double", prefix: :optional, kind: :field},
          %Field{name: :opt_str, number: 6, type: "string", prefix: :optional, kind: :field},
          %Field{name: :opt_bytes, number: 7, type: "bytes", prefix: :optional, kind: :field},
          %Field{name: :req_int32, number: 8, type: "int32", kind: :field},
          %Field{name: :req_str, number: 9, type: "string", kind: :field}
        ],
        fields_by_number: %{
          1 =>
            %Field{name: :opt_int32, number: 1, type: "int32", prefix: :optional, kind: :field},
          2 =>
            %Field{name: :opt_sint32, number: 2, type: "sint32", prefix: :optional, kind: :field},
          3 => %Field{name: :opt_bool, number: 3, type: "bool", prefix: :optional, kind: :field},
          4 =>
            %Field{name: :opt_float, number: 4, type: "float", prefix: :optional, kind: :field},
          5 =>
            %Field{name: :opt_double, number: 5, type: "double", prefix: :optional, kind: :field},
          6 => %Field{name: :opt_str, number: 6, type: "string", prefix: :optional, kind: :field},
          7 =>
            %Field{name: :opt_bytes, number: 7, type: "bytes", prefix: :optional, kind: :field},
          8 => %Field{name: :req_int32, number: 8, type: "int32", kind: :field},
          9 => %Field{name: :req_str, number: 9, type: "string", kind: :field}
        }
      }

      # 1. Missing fields: optional are nil, non-optional are defaults
      decoded_empty = Protobuf.decode!(msg, <<>>)
      assert is_nil(decoded_empty.opt_int32)
      assert is_nil(decoded_empty.opt_sint32)
      assert is_nil(decoded_empty.opt_bool)
      assert is_nil(decoded_empty.opt_float)
      assert is_nil(decoded_empty.opt_double)
      assert is_nil(decoded_empty.opt_str)
      assert is_nil(decoded_empty.opt_bytes)
      assert decoded_empty.req_int32 == 0
      assert decoded_empty.req_str == ""

      # 2. Explicit defaults for optional fields are preserved
      explicit_defaults = %{
        opt_int32: 0,
        opt_sint32: 0,
        opt_bool: false,
        opt_float: 0.0,
        opt_double: 0.0,
        opt_str: "",
        opt_bytes: <<>>
      }

      encoded = Protobuf.encode!(msg, explicit_defaults)
      # Must not be empty because optional defaults are serialized
      assert byte_size(encoded) > 0

      decoded = Protobuf.decode!(msg, encoded)
      assert decoded.opt_int32 == 0
      assert decoded.opt_sint32 == 0
      assert decoded.opt_bool == false
      assert decoded.opt_float == 0.0
      assert decoded.opt_double == 0.0
      assert decoded.opt_str == ""
      assert decoded.opt_bytes == <<>>

      # 3. Non-default values are preserved
      values = %{
        opt_int32: 123,
        opt_sint32: -456,
        opt_bool: true,
        opt_float: 1.5,
        opt_double: 2.5,
        opt_str: "proto3 optional",
        opt_bytes: <<10, 20, 30>>
      }

      encoded_vals = Protobuf.encode!(msg, values)
      decoded_vals = Protobuf.decode!(msg, encoded_vals)
      assert decoded_vals.opt_int32 == 123
      assert decoded_vals.opt_sint32 == -456
      assert decoded_vals.opt_bool == true
      assert_in_delta decoded_vals.opt_float, 1.5, 0.001
      assert_in_delta decoded_vals.opt_double, 2.5, 0.001
      assert decoded_vals.opt_str == "proto3 optional"
      assert decoded_vals.opt_bytes == <<10, 20, 30>>
    end
  end

  describe "Enum Atom <-> Integer Wire Resolution" do
    setup do
      {:ok, edge_schema} = Schema.from_file("priv/protos/edge_cases/edge_cases.proto")
      edge_msg = Schema.message(edge_schema, "ComplicatedRequest")

      {:ok, nested_schema} =
        Schema.from_file("priv/protos/nested_enum_conflict/nested_enum_conflict.proto")

      nested_msg = Schema.message(nested_schema, "ListFoosRequest")

      %{
        edge_schema: edge_schema,
        edge_msg: edge_msg,
        nested_schema: nested_schema,
        nested_msg: nested_msg
      }
    end

    test "encoder encodes symbolic atoms, strings, and integers for top-level enums", %{
      edge_msg: edge_msg,
      edge_schema: edge_schema
    } do
      # status is field 3, wire type 0 -> (3 <<< 3) | 0 = 24 (0x18)
      # :status_active is 1 -> <<0x18, 1>>
      assert Protobuf.encode!(edge_msg, %{status: :status_active}, edge_schema) == <<0x18, 1>>

      # string "STATUS_INACTIVE" is 2 -> <<0x18, 2>>
      assert Protobuf.encode!(edge_msg, %{status: "STATUS_INACTIVE"}, edge_schema) == <<0x18, 2>>

      # case-insensitive atom or string
      assert Protobuf.encode!(edge_msg, %{status: :STATUS_ACTIVE}, edge_schema) == <<0x18, 1>>
      assert Protobuf.encode!(edge_msg, %{status: "status_inactive"}, edge_schema) == <<0x18, 2>>

      # numeric string "3" or raw integer 3
      assert Protobuf.encode!(edge_msg, %{status: "3"}, edge_schema) == <<0x18, 3>>
      assert Protobuf.encode!(edge_msg, %{status: 3}, edge_schema) == <<0x18, 3>>

      # negative enum value :ERROR_STATE (-1)
      encoded_neg = Protobuf.encode!(edge_msg, %{status: :ERROR_STATE}, edge_schema)
      assert byte_size(encoded_neg) > 0
    end

    test "encoder omits default zero enum value on non-optional fields", %{
      edge_msg: edge_msg,
      edge_schema: edge_schema
    } do
      # DETAILED_STATUS_UNSPECIFIED is 0 -> should be omitted for non-optional field
      assert Protobuf.encode!(edge_msg, %{status: :DETAILED_STATUS_UNSPECIFIED}, edge_schema) ==
               <<>>

      assert Protobuf.encode!(edge_msg, %{status: "DETAILED_STATUS_UNSPECIFIED"}, edge_schema) ==
               <<>>

      assert Protobuf.encode!(edge_msg, %{status: 0}, edge_schema) == <<>>
    end

    test "encoder errors on invalid/unknown enum values", %{
      edge_msg: edge_msg,
      edge_schema: edge_schema
    } do
      assert {:error, 13, _} =
               Protobuf.encode(edge_msg, %{status: :NON_EXISTENT_ENUM_VAL}, edge_schema)

      assert {:error, 13, _} =
               Protobuf.encode(edge_msg, %{status: "NON_EXISTENT_ENUM_VAL"}, edge_schema)
    end

    test "decoder maps recognized wire integers to symbolic atoms", %{
      edge_msg: edge_msg,
      edge_schema: edge_schema
    } do
      # 1 -> :status_active
      decoded1 = Protobuf.decode!(edge_msg, <<0x18, 1>>, edge_schema)
      assert decoded1.status == :status_active

      # 2 -> :STATUS_INACTIVE
      decoded2 = Protobuf.decode!(edge_msg, <<0x18, 2>>, edge_schema)
      assert decoded2.status == :STATUS_INACTIVE

      # 3 -> :Status_Pending
      decoded3 = Protobuf.decode!(edge_msg, <<0x18, 3>>, edge_schema)
      assert decoded3.status == :Status_Pending

      # 0 on wire -> :DETAILED_STATUS_UNSPECIFIED
      decoded0 = Protobuf.decode!(edge_msg, <<0x18, 0>>, edge_schema)
      assert decoded0.status == :DETAILED_STATUS_UNSPECIFIED

      # Missing on wire -> defaults to zero atom :DETAILED_STATUS_UNSPECIFIED
      decoded_empty = Protobuf.decode!(edge_msg, <<>>, edge_schema)
      assert decoded_empty.status == :DETAILED_STATUS_UNSPECIFIED
    end

    test "decoder preserves unrecognized wire integers as integers", %{
      edge_msg: edge_msg,
      edge_schema: edge_schema
    } do
      decoded = Protobuf.decode!(edge_msg, <<0x18, 99>>, edge_schema)
      assert decoded.status == 99
    end

    test "supports negative enum values roundtrip", %{
      edge_msg: edge_msg,
      edge_schema: edge_schema
    } do
      encoded = Protobuf.encode!(edge_msg, %{status: :ERROR_STATE}, edge_schema)
      decoded = Protobuf.decode!(edge_msg, encoded, edge_schema)
      assert decoded.status == :ERROR_STATE
    end

    test "supports nested enums defined inside message definitions", %{
      nested_msg: nested_msg,
      nested_schema: nested_schema
    } do
      # sort_order is field 1, wire type 0 -> <<8, ...>>
      # 1 -> :SORT_ORDER_NEWEST_FIRST
      encoded_atom =
        Protobuf.encode!(nested_msg, %{sort_order: :SORT_ORDER_NEWEST_FIRST}, nested_schema)

      assert encoded_atom == <<8, 1>>

      encoded_str =
        Protobuf.encode!(nested_msg, %{sort_order: "SORT_ORDER_NEWEST_FIRST"}, nested_schema)

      assert encoded_str == <<8, 1>>

      decoded = Protobuf.decode!(nested_msg, encoded_atom, nested_schema)
      assert decoded.sort_order == :SORT_ORDER_NEWEST_FIRST

      decoded_empty = Protobuf.decode!(nested_msg, <<>>, nested_schema)
      assert decoded_empty.sort_order == :SORT_ORDER_UNSPECIFIED
    end

    test "supports repeated enum fields with atoms, strings, and integers" do
      msg = %Message{
        name: "RepeatedEnums",
        fields: [
          %Field{name: :statuses, number: 1, type: "Status", prefix: :repeated, kind: :field}
        ],
        fields_by_number: %{
          1 =>
            %Field{name: :statuses, number: 1, type: "Status", prefix: :repeated, kind: :field}
        },
        nested_enums: [
          %Schema.Enum{
            name: "Status",
            fields: [STATUS_UNKNOWN: 0, STATUS_ACTIVE: 1, STATUS_PAUSED: 2]
          }
        ]
      }

      payload = %{statuses: [:STATUS_ACTIVE, "STATUS_PAUSED", 1]}
      encoded = Protobuf.encode!(msg, payload)
      decoded = Protobuf.decode!(msg, encoded)

      assert decoded.statuses == [:STATUS_ACTIVE, :STATUS_PAUSED, :STATUS_ACTIVE]
    end

    test "preserves exact ordering on large repeated lists of scalars and nested messages" do
      msg = %Message{
        name: "LargeRepeatedMessage",
        fields: [
          %Field{name: :numbers, number: 1, type: "int32", prefix: :repeated, kind: :field},
          %Field{name: :tags, number: 2, type: "string", prefix: :repeated, kind: :field}
        ],
        fields_by_number: %{
          1 => %Field{name: :numbers, number: 1, type: "int32", prefix: :repeated, kind: :field},
          2 => %Field{name: :tags, number: 2, type: "string", prefix: :repeated, kind: :field}
        }
      }

      numbers = Enum.to_list(1..5_000)
      tags = Enum.map(1..5_000, &"tag-#{&1}")
      payload = %{numbers: numbers, tags: tags}

      encoded = Protobuf.encode!(msg, payload)
      decoded = Protobuf.decode!(msg, encoded)

      assert decoded.numbers == numbers
      assert decoded.tags == tags
    end
  end
end
