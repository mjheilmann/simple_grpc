defmodule SimpleGrpc.Protobuf do
  @moduledoc """
  A pure-Elixir, schema-driven Protobuf binary encoder and decoder.

  Operates directly on `SimpleGrpc.Schema.Message` descriptors, plain Elixir maps,
  and message structs with zero external dependencies.
  """

  import Bitwise

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.{Message, Field}

  @doc """
  Encodes an Elixir map, keyword list, or struct into protobuf binary format
  using the provided message descriptor (or module with `__fields__/0`).
  """
  def encode(message_or_mod, data, schema_ctx \\ nil) do
    {:ok, encode!(message_or_mod, data, schema_ctx)}
  rescue
    e in [ArgumentError, RuntimeError] ->
      {:error, Exception.message(e)}
  end

  @doc """
  Encodes data into protobuf binary format, raising on error.
  """
  def encode!(message_or_mod, data, schema_ctx \\ nil) do
    msg_desc = resolve_message_descriptor(message_or_mod, schema_ctx)
    plain_map = to_plain_map(data)

    encode_fields(msg_desc.fields, plain_map, schema_ctx)
  end

  @doc """
  Decodes a protobuf binary into an Elixir map using the provided message descriptor.
  """
  def decode(message_or_mod, binary, schema_ctx \\ nil) do
    {:ok, decode!(message_or_mod, binary, schema_ctx)}
  rescue
    e in [ArgumentError, RuntimeError] ->
      {:error, Exception.message(e)}
  end

  @doc """
  Decodes a protobuf binary into an Elixir map, raising on error.
  """
  def decode!(message_or_mod, binary, schema_ctx \\ nil) when is_binary(binary) do
    msg_desc = resolve_message_descriptor(message_or_mod, schema_ctx)
    decoded_raw = decode_wire_fields(binary, msg_desc, schema_ctx, %{})

    apply_defaults_and_coerce(msg_desc, decoded_raw)
  end

  # ---------------------------------------------------------------------------
  # Encoding Implementation
  # ---------------------------------------------------------------------------

  defp encode_fields(fields, data, schema_ctx) do
    Enum.reduce(fields, <<>>, fn field, acc ->
      case field.kind do
        :oneof ->
          acc <> encode_oneof(field, data, schema_ctx)

        :map ->
          val = Map.get(data, field.name)
          if is_map(val) and map_size(val) > 0 do
            acc <> encode_map_entries(field, val, schema_ctx)
          else
            acc
          end

        _ ->
          val = Map.get(data, field.name)

          if field.prefix == :repeated do
            acc <> encode_repeated(field, val, schema_ctx)
          else
            if is_nil(val) or is_default_value?(val, field.type) do
              acc
            else
              acc <> encode_single_field(field.number, field.type, val, schema_ctx)
            end
          end
      end
    end)
  end

  defp encode_oneof(%Field{fields: oneof_fields}, data, schema_ctx) do
    Enum.find_value(oneof_fields, <<>>, fn subfield ->
      val = Map.get(data, subfield.name)

      if not is_nil(val) and not is_default_value?(val, subfield.type) do
        encode_single_field(subfield.number, subfield.type, val, schema_ctx)
      end
    end) || <<>>
  end

  defp encode_repeated(_field, nil, _schema_ctx), do: <<>>
  defp encode_repeated(_field, [], _schema_ctx), do: <<>>

  defp encode_repeated(field, list, schema_ctx) when is_list(list) do
    if is_packable_type?(field.type) do
      # Proto3 packed repeated encoding
      packed_bytes =
        Enum.reduce(list, <<>>, fn item, acc ->
          acc <> encode_raw_value(field.type, item, schema_ctx)
        end)

      tag = encode_tag(field.number, 2)
      tag <> encode_varint(byte_size(packed_bytes)) <> packed_bytes
    else
      # Unpacked repeated encoding
      Enum.reduce(list, <<>>, fn item, acc ->
        acc <> encode_single_field(field.number, field.type, item, schema_ctx)
      end)
    end
  end

  defp encode_map_entries(field, map, schema_ctx) when is_map(map) do
    Enum.reduce(map, <<>>, fn {k, v}, acc ->
      k_bytes = encode_single_field(1, field.key_type, k, schema_ctx)
      v_bytes = encode_single_field(2, field.val_type, v, schema_ctx)
      entry_payload = k_bytes <> v_bytes

      tag = encode_tag(field.number, 2)
      acc <> tag <> encode_varint(byte_size(entry_payload)) <> entry_payload
    end)
  end

  defp encode_single_field(field_num, type, value, schema_ctx) do
    wire_type = wire_type_for(type)
    tag = encode_tag(field_num, wire_type)

    case wire_type do
      0 ->
        tag <> encode_varint_type(type, value)

      1 ->
        tag <> encode_64bit(type, value)

      2 ->
        payload = encode_length_delimited_payload(type, value, schema_ctx)
        tag <> encode_varint(byte_size(payload)) <> payload

      5 ->
        tag <> encode_32bit(type, value)
    end
  end

  defp encode_raw_value(type, value, schema_ctx) do
    case wire_type_for(type) do
      0 -> encode_varint_type(type, value)
      1 -> encode_64bit(type, value)
      5 -> encode_32bit(type, value)
      2 ->
        payload = encode_length_delimited_payload(type, value, schema_ctx)
        encode_varint(byte_size(payload)) <> payload
    end
  end

  defp encode_length_delimited_payload("string", value, _schema_ctx) when is_binary(value), do: value
  defp encode_length_delimited_payload("string", value, _schema_ctx) when is_atom(value), do: Atom.to_string(value)
  defp encode_length_delimited_payload("bytes", value, _schema_ctx) when is_binary(value), do: value

  defp encode_length_delimited_payload(type, value, schema_ctx) do
    # Embedded sub-message
    sub_desc = resolve_submessage_descriptor(type, schema_ctx)
    encode!(sub_desc, value, schema_ctx)
  end

  defp encode_tag(field_num, wire_type) do
    encode_varint((field_num <<< 3) ||| wire_type)
  end

  defp encode_varint_type("bool", true), do: <<1>>
  defp encode_varint_type("bool", false), do: <<0>>
  defp encode_varint_type("sint32", val) when is_integer(val), do: encode_varint(encode_zigzag32(val))
  defp encode_varint_type("sint64", val) when is_integer(val), do: encode_varint(encode_zigzag64(val))
  defp encode_varint_type(_type, val) when is_integer(val), do: encode_varint(val)
  defp encode_varint_type(_type, val) when is_atom(val), do: encode_varint(0)

  defp encode_64bit("double", val) when is_number(val), do: <<val * 1.0::float-little-size(64)>>
  defp encode_64bit("fixed64", val) when is_integer(val), do: <<val::little-unsigned-size(64)>>
  defp encode_64bit("sfixed64", val) when is_integer(val), do: <<val::little-signed-size(64)>>
  defp encode_64bit(_type, val) when is_integer(val), do: <<val::little-unsigned-size(64)>>

  defp encode_32bit("float", val) when is_number(val), do: <<val * 1.0::float-little-size(32)>>
  defp encode_32bit("fixed32", val) when is_integer(val), do: <<val::little-unsigned-size(32)>>
  defp encode_32bit("sfixed32", val) when is_integer(val), do: <<val::little-signed-size(32)>>
  defp encode_32bit(_type, val) when is_integer(val), do: <<val::little-unsigned-size(32)>>

  # ---------------------------------------------------------------------------
  # Decoding Implementation
  # ---------------------------------------------------------------------------

  defp decode_wire_fields(<<>>, _msg_desc, _schema_ctx, acc), do: acc

  defp decode_wire_fields(binary, msg_desc, schema_ctx, acc) do
    {tag, rest} = decode_varint(binary)
    field_num = tag >>> 3
    wire_type = tag &&& 0x07

    field_desc = Message.field_by_number(msg_desc, field_num)

    if is_nil(field_desc) do
      # Unknown field: skip bytes per wire type
      {_skipped, remaining} = skip_wire_value(rest, wire_type)
      decode_wire_fields(remaining, msg_desc, schema_ctx, acc)
    else
      {field_value, remaining} = decode_field_value(rest, wire_type, field_desc, schema_ctx)
      updated_acc = store_decoded_field(acc, field_desc, field_value)
      decode_wire_fields(remaining, msg_desc, schema_ctx, updated_acc)
    end
  end

  defp decode_field_value(binary, 2, %Field{kind: :map} = field, schema_ctx) do
    {len, rest} = decode_varint(binary)
    <<map_entry_bytes::binary-size(^len), remaining::binary>> = rest

    # Map entry is a sub-message with field 1 (key) and field 2 (value)
    entry_desc = %Message{
      name: "MapEntry",
      fields: [
        %Field{name: :key, number: 1, type: field.key_type, kind: :field},
        %Field{name: :value, number: 2, type: field.val_type, kind: :field}
      ],
      fields_by_number: %{
        1 => %Field{name: :key, number: 1, type: field.key_type, kind: :field},
        2 => %Field{name: :value, number: 2, type: field.val_type, kind: :field}
      }
    }

    decoded_entry = decode_wire_fields(map_entry_bytes, entry_desc, schema_ctx, %{})
    k = Map.get(decoded_entry, :key, default_scalar_value(field.key_type))
    v = Map.get(decoded_entry, :value, default_scalar_value(field.val_type))

    {{k, v}, remaining}
  end

  defp decode_field_value(binary, 2, %Field{prefix: :repeated} = field, schema_ctx) do
    # Could be packed scalar or length-delimited item (string/bytes/message)
    if is_packable_type?(field.type) do
      {len, rest} = decode_varint(binary)
      <<packed_bytes::binary-size(^len), remaining::binary>> = rest
      items = decode_packed_values(packed_bytes, field.type)
      {items, remaining}
    else
      # Single item of repeated string/bytes/message
      {item, remaining} = decode_scalar_or_message(binary, 2, field.type, schema_ctx)
      {[item], remaining}
    end
  end

  defp decode_field_value(binary, wire_type, field, schema_ctx) do
    decode_scalar_or_message(binary, wire_type, field.type, schema_ctx)
  end

  defp decode_scalar_or_message(binary, 0, "bool", _schema_ctx) do
    {val, rest} = decode_varint(binary)
    {val != 0, rest}
  end

  defp decode_scalar_or_message(binary, 0, "sint32", _schema_ctx) do
    {val, rest} = decode_varint(binary)
    {decode_zigzag32(val), rest}
  end

  defp decode_scalar_or_message(binary, 0, "sint64", _schema_ctx) do
    {val, rest} = decode_varint(binary)
    {decode_zigzag64(val), rest}
  end

  defp decode_scalar_or_message(binary, 0, _type, _schema_ctx) do
    decode_varint(binary)
  end

  defp decode_scalar_or_message(<<val::little-unsigned-size(64), rest::binary>>, 1, "fixed64", _schema_ctx), do: {val, rest}
  defp decode_scalar_or_message(<<val::little-signed-size(64), rest::binary>>, 1, "sfixed64", _schema_ctx), do: {val, rest}
  defp decode_scalar_or_message(<<val::float-little-size(64), rest::binary>>, 1, "double", _schema_ctx), do: {val, rest}
  defp decode_scalar_or_message(<<val::little-unsigned-size(64), rest::binary>>, 1, _type, _schema_ctx), do: {val, rest}

  defp decode_scalar_or_message(<<val::little-unsigned-size(32), rest::binary>>, 5, "fixed32", _schema_ctx), do: {val, rest}
  defp decode_scalar_or_message(<<val::little-signed-size(32), rest::binary>>, 5, "sfixed32", _schema_ctx), do: {val, rest}
  defp decode_scalar_or_message(<<val::float-little-size(32), rest::binary>>, 5, "float", _schema_ctx), do: {val, rest}
  defp decode_scalar_or_message(<<val::little-unsigned-size(32), rest::binary>>, 5, _type, _schema_ctx), do: {val, rest}

  defp decode_scalar_or_message(binary, 2, "string", _schema_ctx) do
    {len, rest} = decode_varint(binary)
    <<str::binary-size(^len), remaining::binary>> = rest
    {str, remaining}
  end

  defp decode_scalar_or_message(binary, 2, "bytes", _schema_ctx) do
    {len, rest} = decode_varint(binary)
    <<bytes::binary-size(^len), remaining::binary>> = rest
    {bytes, remaining}
  end

  defp decode_scalar_or_message(binary, 2, type, schema_ctx) do
    # Embedded submessage
    {len, rest} = decode_varint(binary)
    <<sub_bytes::binary-size(^len), remaining::binary>> = rest
    sub_desc = resolve_submessage_descriptor(type, schema_ctx)
    sub_decoded = decode!(sub_desc, sub_bytes, schema_ctx)
    {sub_decoded, remaining}
  end

  defp decode_packed_values(<<>>, _type), do: []

  defp decode_packed_values(binary, type) do
    {item, rest} = decode_scalar_or_message(binary, wire_type_for(type), type, nil)
    [item | decode_packed_values(rest, type)]
  end

  defp store_decoded_field(acc, %Field{kind: :map} = field, {k, v}) do
    existing_map = Map.get(acc, field.name, %{})
    Map.put(acc, field.name, Map.put(existing_map, k, v))
  end

  defp store_decoded_field(acc, %Field{prefix: :repeated} = field, items) when is_list(items) do
    existing_list = Map.get(acc, field.name, [])
    Map.put(acc, field.name, existing_list ++ items)
  end

  defp store_decoded_field(acc, field, value) do
    Map.put(acc, field.name, value)
  end

  defp skip_wire_value(binary, 0) do
    {_val, rest} = decode_varint(binary)
    {:ok, rest}
  end

  defp skip_wire_value(<<_::64, rest::binary>>, 1), do: {:ok, rest}

  defp skip_wire_value(binary, 2) do
    {len, rest} = decode_varint(binary)
    <<_::binary-size(^len), remaining::binary>> = rest
    {:ok, remaining}
  end

  defp skip_wire_value(<<_::32, rest::binary>>, 5), do: {:ok, rest}

  # ---------------------------------------------------------------------------
  # Default Values & Coercion
  # ---------------------------------------------------------------------------

  defp apply_defaults_and_coerce(%Message{fields: fields}, raw_map) do
    Enum.reduce(fields, %{}, fn field, acc ->
      case field.kind do
        :oneof ->
          Enum.reduce(field.fields, acc, fn oneof_subfield, inner_acc ->
            if Map.has_key?(raw_map, oneof_subfield.name) do
              Map.put(inner_acc, oneof_subfield.name, Map.get(raw_map, oneof_subfield.name))
            else
              inner_acc
            end
          end)

        :map ->
          val = Map.get(raw_map, field.name, %{})
          Map.put(acc, field.name, val)

        _ ->
          if field.prefix == :repeated do
            val = Map.get(raw_map, field.name, [])
            Map.put(acc, field.name, val)
          else
            val = Map.get(raw_map, field.name, default_scalar_value(field.type))
            Map.put(acc, field.name, val)
          end
      end
    end)
  end

  defp default_scalar_value("string"), do: ""
  defp default_scalar_value("bytes"), do: <<>>
  defp default_scalar_value("bool"), do: false
  defp default_scalar_value(t) when t in ~w(int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64), do: 0
  defp default_scalar_value(t) when t in ~w(float double), do: 0.0
  defp default_scalar_value(_), do: nil

  defp is_default_value?(val, "string"), do: val == ""
  defp is_default_value?(val, "bytes"), do: val == <<>>
  defp is_default_value?(val, "bool"), do: val == false
  defp is_default_value?(val, t) when t in ~w(int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64), do: val == 0
  defp is_default_value?(val, t) when t in ~w(float double), do: val == 0.0 or val == 0
  defp is_default_value?(val, _type), do: is_nil(val)

  # ---------------------------------------------------------------------------
  # Varint & Bitwise Utilities
  # ---------------------------------------------------------------------------

  @doc """
  Encodes an integer into standard protobuf varint bytes.
  """
  def encode_varint(n) when n >= 0 and n < 128, do: <<n::8>>

  def encode_varint(n) when n >= 0 do
    <<1::1, (n &&& 0x7F)::7, encode_varint(n >>> 7)::binary>>
  end

  def encode_varint(n) when n < 0 do
    encode_varint(n &&& 0xFFFFFFFFFFFFFFFF)
  end

  @doc """
  Decodes varint bytes into `{integer, remaining_binary}`.
  """
  def decode_varint(binary, acc \\ 0, shift \\ 0)

  def decode_varint(<<0::1, val::7, rest::binary>>, acc, shift) do
    {acc ||| (val <<< shift), rest}
  end

  def decode_varint(<<1::1, val::7, rest::binary>>, acc, shift) do
    decode_varint(rest, acc ||| (val <<< shift), shift + 7)
  end

  def decode_varint(<<>>, _acc, _shift) do
    raise ArgumentError, "Unexpected EOF while decoding varint"
  end

  @doc """
  Encodes an integer using 32-bit ZigZag encoding (for sint32).
  """
  def encode_zigzag32(n) do
    bxor(n <<< 1, n >>> 31) &&& 0xFFFFFFFF
  end

  @doc """
  Decodes a 32-bit ZigZag encoded integer.
  """
  def decode_zigzag32(n) do
    bxor(n >>> 1, -(n &&& 1))
  end

  @doc """
  Encodes an integer using 64-bit ZigZag encoding (for sint64).
  """
  def encode_zigzag64(n) do
    bxor(n <<< 1, n >>> 63) &&& 0xFFFFFFFFFFFFFFFF
  end

  @doc """
  Decodes a 64-bit ZigZag encoded integer.
  """
  def decode_zigzag64(n) do
    bxor(n >>> 1, -(n &&& 1))
  end

  # ---------------------------------------------------------------------------
  # Wire Type Helpers
  # ---------------------------------------------------------------------------

  def wire_type_for(type) when type in ~w(int32 int64 uint32 uint64 sint32 sint64 bool enum), do: 0
  def wire_type_for(type) when type in ~w(fixed64 sfixed64 double), do: 1
  def wire_type_for(type) when type in ~w(string bytes) or is_tuple(type), do: 2
  def wire_type_for(type) when type in ~w(fixed32 sfixed32 float), do: 5
  def wire_type_for(_message_type), do: 2

  def is_packable_type?(type) do
    type in ~w(int32 int64 uint32 uint64 sint32 sint64 bool enum fixed64 sfixed64 double fixed32 sfixed32 float)
  end

  # ---------------------------------------------------------------------------
  # Descriptor & Data Resolvers
  # ---------------------------------------------------------------------------

  defp resolve_message_descriptor(%Message{} = msg, _ctx), do: msg

  defp resolve_message_descriptor(module, _ctx) when is_atom(module) do
    if function_exported?(module, :__fields__, 0) do
      fields = module.__fields__()
      name = if function_exported?(module, :__message_name__, 0), do: module.__message_name__(), else: inspect(module)

      fields_by_num =
        Enum.flat_map(fields, fn
          %Field{kind: :oneof, fields: oneof_fields} -> Enum.map(oneof_fields, &{&1.number, &1})
          %Field{number: num} = f when is_integer(num) -> [{num, f}]
          _ -> []
        end)
        |> Map.new()

      %Message{name: name, fields: fields, fields_by_number: fields_by_num}
    else
      raise ArgumentError, "Module #{inspect(module)} does not implement protobuf __fields__/0"
    end
  end

  defp resolve_submessage_descriptor({:message, type_name}, schema_ctx) do
    resolve_submessage_descriptor(type_name, schema_ctx)
  end

  defp resolve_submessage_descriptor(type_name, %Schema{} = schema) when is_binary(type_name) do
    Schema.message(schema, type_name) ||
      raise ArgumentError, "Message #{inspect(type_name)} not found in schema"
  end

  defp resolve_submessage_descriptor(type_name, _ctx) when is_binary(type_name) do
    raise ArgumentError, "Cannot resolve nested message #{inspect(type_name)} without a %SimpleGrpc.Schema{} context"
  end

  defp to_plain_map(%{__struct__: _} = s), do: Map.from_struct(s)

  defp to_plain_map(map) when is_map(map) do
    Enum.reduce(map, %{}, fn {k, v}, acc ->
      atom_key = if is_binary(k), do: String.to_atom(k), else: k
      Map.put(acc, atom_key, v)
    end)
  end

  defp to_plain_map(kw) when is_list(kw) do
    Enum.reduce(kw, %{}, fn {k, v}, acc ->
      atom_key = if is_binary(k), do: String.to_atom(k), else: k
      Map.put(acc, atom_key, v)
    end)
  end
end
