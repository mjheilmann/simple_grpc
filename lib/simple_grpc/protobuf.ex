defmodule SimpleGrpc.Protobuf do
  @moduledoc """
  A pure-Elixir, schema-driven Protobuf binary encoder and decoder.

  Operates directly on `SimpleGrpc.Schema.Message` descriptors, plain Elixir maps,
  and message structs with zero external dependencies.
  """

  import Bitwise

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.{Message, Field}

  require Logger

  @doc """
  Encodes an Elixir map, keyword list, or struct into protobuf binary format
  using the provided message descriptor (or module with `__fields__/0`).
  """
  def encode(message_or_mod, data, schema_ctx \\ nil) do
    {:ok, encode!(message_or_mod, data, schema_ctx)}
  catch
    :throw, {:error, msg} ->
      Logger.error("[SimpleGrpc.Protobuf] encode failed: #{msg}")
      {:error, 13, "internal error"}
  end

  @doc """
  Encodes data into protobuf binary format, raising on error.
  """
  def encode!(message_or_mod, data, schema_ctx \\ nil) do
    schema_ctx = extract_schema_ctx(schema_ctx, message_or_mod)

    case schema_ctx do
      %Schema{compiled_module: mod} when not is_nil(mod) ->
        msg_name = message_name(message_or_mod)

        case mod.encode(msg_name, data) do
          :error ->
            msg_desc = resolve_message_descriptor(message_or_mod, schema_ctx)

            encode_fields(msg_desc.fields, data, schema_ctx, msg_desc)
            |> IO.iodata_to_binary()

          iodata ->
            IO.iodata_to_binary(iodata)
        end

      _ ->
        msg_desc = resolve_message_descriptor(message_or_mod, schema_ctx)

        encode_fields(msg_desc.fields, data, schema_ctx, msg_desc)
        |> IO.iodata_to_binary()
    end
  end

  @doc """
  Decodes a protobuf binary into an Elixir map using the provided message descriptor.
  """
  def decode(message_or_mod, binary, schema_ctx \\ nil) do
    {:ok, decode!(message_or_mod, binary, schema_ctx)}
  catch
    :throw, {:error, msg} ->
      {:error, 3, "bad request: #{msg}"}
  end

  @doc """
  Decodes a protobuf binary into an Elixir map, raising on error.
  """
  def decode!(message_or_mod, binary, schema_ctx \\ nil)

  def decode!(message_or_mod, binary, %Schema{compiled_module: mod} = schema_ctx)
      when is_binary(binary) and not is_nil(mod) do
    msg_name = message_name(message_or_mod)

    case mod.decode(msg_name, binary) do
      :error ->
        decode_fallback(message_or_mod, binary, schema_ctx)

      res when is_map(res) ->
        res
    end
  end

  def decode!(%Message{default_map: def_map} = msg_desc, binary, schema_ctx)
      when is_binary(binary) and is_map(def_map) do
    decode_with_descriptor(msg_desc, binary, schema_ctx)
  end

  def decode!(message_or_mod, binary, schema_ctx) when is_binary(binary) do
    decode_fallback(message_or_mod, binary, schema_ctx)
  end

  defp decode_with_descriptor(
         %Message{default_map: def_map, repeated_fields: rep_fields} = msg_desc,
         binary,
         schema_ctx
       ) do
    schema_ctx = extract_schema_ctx(schema_ctx, msg_desc)
    raw_map = decode_wire_fields(binary, msg_desc, schema_ctx, %{})

    merged = Map.merge(def_map, raw_map)

    res =
      case rep_fields do
        [] ->
          merged

        [single_rep] ->
          case Map.get(raw_map, single_rep) do
            nil -> merged
            val -> Map.put(merged, single_rep, Enum.reverse(val))
          end

        multiple_rep ->
          Enum.reduce(multiple_rep, merged, fn f_name, acc ->
            case Map.get(raw_map, f_name) do
              nil -> acc
              val -> Map.put(acc, f_name, Enum.reverse(val))
            end
          end)
      end

    if msg_desc.required_fields != [] do
      for req_f <- msg_desc.required_fields do
        if is_nil(Map.get(res, req_f)) do
          throw({:error, "missing required field '#{req_f}' for #{msg_desc.name}"})
        end
      end
    end

    stamp_struct(res, msg_desc.struct_module)
  end

  defp decode_fallback(message_or_mod, binary, schema_ctx) do
    schema_ctx = extract_schema_ctx(schema_ctx, message_or_mod)
    msg_desc = resolve_message_descriptor(message_or_mod, schema_ctx)

    if is_map(msg_desc.default_map) do
      decode_with_descriptor(msg_desc, binary, schema_ctx)
    else
      decoded_raw = decode_wire_fields(binary, msg_desc, schema_ctx, %{})
      res = apply_defaults_and_coerce(msg_desc, decoded_raw, schema_ctx)

      if msg_desc.required_fields != [] do
        for req_f <- msg_desc.required_fields do
          if is_nil(Map.get(res, req_f)) do
            throw({:error, "missing required field '#{req_f}' for #{msg_desc.name}"})
          end
        end
      end

      stamp_struct(res, msg_desc.struct_module)
    end
  end

  # The compiled codec starts from the struct as `default_map`; the interpreted one decodes
  # into a plain map and turns it into the message's struct here.
  defp stamp_struct(decoded, nil), do: decoded

  defp stamp_struct(decoded, module) do
    decoded |> Map.put_new(:__unknown_fields__, []) |> Map.put(:__struct__, module)
  end

  defp message_name(name) when is_binary(name), do: name
  defp message_name(%Message{full_name: full_name}) when is_binary(full_name), do: full_name
  defp message_name(%Message{name: name}) when is_binary(name), do: name
  defp message_name(mod) when is_atom(mod), do: to_string(mod)
  defp message_name(_), do: ""

  # ---------------------------------------------------------------------------
  # Encoding Implementation
  # ---------------------------------------------------------------------------

  defp encode_fields(fields, data, schema_ctx, msg_desc) when is_map(data) do
    field_iodata =
      Enum.map(fields, fn
        %Field{kind: :oneof} = field ->
          encode_oneof(field, data, schema_ctx, msg_desc)

        %Field{kind: :map} = field ->
          val = get_field_val(data, field.name)

          if is_map(val) and map_size(val) > 0 do
            encode_map_entries(field, val, schema_ctx, msg_desc)
          else
            <<>>
          end

        %Field{prefix: :repeated} = field ->
          val = get_field_val(data, field.name)
          encode_repeated(field, val, schema_ctx, msg_desc)

        %Field{prefix: :required} = field ->
          case get_field_val(data, field.name) do
            nil ->
              throw({:error, "missing required field '#{field.name}' for #{msg_desc.name}"})

            val ->
              encode_field_value(field, val, schema_ctx, msg_desc)
          end

        %Field{prefix: :optional} = field ->
          case get_field_val(data, field.name) do
            nil -> <<>>
            val -> encode_field_value(field, val, schema_ctx, msg_desc)
          end

        %Field{} = field ->
          case get_field_val(data, field.name) do
            nil ->
              <<>>

            val ->
              if is_field_default?(field, val, schema_ctx, msg_desc) do
                <<>>
              else
                encode_field_value(field, val, schema_ctx, msg_desc)
              end
          end
      end)

    unknown_iodata =
      case Map.get(data, :__unknown_fields__) || Map.get(data, "__unknown_fields__") do
        nil -> []
        bin when is_binary(bin) -> bin
        list when is_list(list) -> list
      end

    [field_iodata, unknown_iodata]
  end

  defp encode_fields(fields, data, schema_ctx, msg_desc) when is_list(data) do
    encode_fields(fields, kw_to_map(data), schema_ctx, msg_desc)
  end

  defp encode_fields(_fields, _data, _schema_ctx, _msg_desc), do: []

  defp get_field_val(data, atom_name) do
    case data do
      %{^atom_name => v} ->
        v

      %{} ->
        str_name = Atom.to_string(atom_name)

        case data do
          %{^str_name => v} -> v
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp is_field_default?(
         %Field{enum_desc: %Schema.Enum{zero_atom: zero_atom, str_to_int: s2i, atom_to_int: a2i}},
         val,
         _schema_ctx,
         _msg_desc
       ) do
    val == zero_atom or val == 0 or val == "0" or
      (zero_atom != nil and val == Atom.to_string(zero_atom)) or
      Map.get(s2i, val) == 0 or (is_atom(val) and Map.get(a2i, val) == 0)
  end

  defp is_field_default?(%Field{default_value: def_val}, val, _schema_ctx, _msg_desc)
       when def_val != nil and not is_binary(val) and not is_atom(val) do
    val == def_val or (is_number(def_val) and is_number(val) and val == 0)
  end

  defp is_field_default?(%Field{default_value: def_val, type: type}, val, _schema_ctx, _msg_desc)
       when def_val != nil and type in ["string", "bytes"] do
    val == def_val
  end

  defp is_field_default?(field, val, schema_ctx, msg_desc) do
    is_default_value?(val, field.type, schema_ctx, msg_desc)
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "string", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_binary(val) do
    [tag || encode_tag(num, 2), encode_varint(byte_size(val)), val]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "string", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_atom(val) do
    str = Atom.to_string(val)
    [tag || encode_tag(num, 2), encode_varint(byte_size(str)), str]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "bytes", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_binary(val) do
    [tag || encode_tag(num, 2), encode_varint(byte_size(val)), val]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, submessage_desc: %Message{} = sub_desc, number: num},
         val,
         schema_ctx,
         _msg_desc
       )
       when is_map(val) or is_list(val) do
    payload = encode_fields(sub_desc.fields, val, schema_ctx, sub_desc)
    [tag || encode_tag(num, 2), encode_varint(IO.iodata_length(payload)), payload]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, enum_desc: %Schema.Enum{} = enum_desc, number: num},
         val,
         _schema_ctx,
         _msg_desc
       ) do
    int_val = resolve_enum_value_to_integer(val, enum_desc)
    [tag || encode_tag(num, 0), encode_varint(int_val)]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "double", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_number(val) do
    [tag || encode_tag(num, 1), <<val * 1.0::float-little-size(64)>>]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "float", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_number(val) do
    [tag || encode_tag(num, 5), <<val * 1.0::float-little-size(32)>>]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "bool", number: num},
         true,
         _schema_ctx,
         _msg_desc
       ),
       do: [tag || encode_tag(num, 0), <<1>>]

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "bool", number: num},
         false,
         _schema_ctx,
         _msg_desc
       ),
       do: [tag || encode_tag(num, 0), <<0>>]

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "sint32", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_integer(val) do
    [tag || encode_tag(num, 0), encode_varint(encode_zigzag32(val))]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "sint64", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_integer(val) do
    [tag || encode_tag(num, 0), encode_varint(encode_zigzag64(val))]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: t, number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_integer(val) and t in ["int32", "int64", "uint32", "uint64"] do
    [tag || encode_tag(num, 0), encode_varint(val)]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "fixed64", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_integer(val) do
    [tag || encode_tag(num, 1), <<val::little-unsigned-size(64)>>]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "sfixed64", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_integer(val) do
    [tag || encode_tag(num, 1), <<val::little-signed-size(64)>>]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "fixed32", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_integer(val) do
    [tag || encode_tag(num, 5), <<val::little-unsigned-size(32)>>]
  end

  defp encode_field_value(
         %Field{tag_bytes: tag, type: "sfixed32", number: num},
         val,
         _schema_ctx,
         _msg_desc
       )
       when is_integer(val) do
    [tag || encode_tag(num, 5), <<val::little-signed-size(32)>>]
  end

  defp encode_field_value(%Field{} = field, val, schema_ctx, msg_desc) do
    encode_single_field(
      field.number,
      field.type,
      val,
      schema_ctx,
      msg_desc,
      field.tag_bytes,
      field.wire_type
    )
  end

  defp encode_oneof(%Field{fields: oneof_fields}, data, schema_ctx, msg_desc) do
    Enum.find_value(oneof_fields, <<>>, fn subfield ->
      val = Map.get(data, subfield.name)

      if not is_nil(val) do
        encode_field_value(subfield, val, schema_ctx, msg_desc)
      end
    end) || <<>>
  end

  @doc false
  def encode_repeated(_field, nil, _schema_ctx, _msg_desc), do: <<>>
  def encode_repeated(_field, [], _schema_ctx, _msg_desc), do: <<>>

  # Enum values come from the field's own linked descriptor, so no schema context is needed
  # (the compiled codec calls this with none).
  def encode_repeated(
        %Field{enum_desc: %Schema.Enum{} = enum_desc, is_packable: true} = field,
        list,
        _schema_ctx,
        _msg_desc
      )
      when is_list(list) do
    packed = Enum.map(list, &encode_varint(resolve_enum_value_to_integer(&1, enum_desc)))
    [encode_tag(field.number, 2), encode_varint(IO.iodata_length(packed)), packed]
  end

  def encode_repeated(
        %Field{enum_desc: %Schema.Enum{} = enum_desc, is_packable: false} = field,
        list,
        _schema_ctx,
        _msg_desc
      )
      when is_list(list) do
    tag = encode_tag(field.number, 0)

    Enum.map(list, &[tag, encode_varint(resolve_enum_value_to_integer(&1, enum_desc))])
  end

  def encode_repeated(%Field{is_packable: true} = field, list, schema_ctx, msg_desc)
      when is_list(list) do
    packed_bytes =
      Enum.map(list, fn item ->
        encode_raw_value(field.type, item, schema_ctx, msg_desc)
      end)

    tag = encode_tag(field.number, 2)
    [tag, encode_varint(IO.iodata_length(packed_bytes)), packed_bytes]
  end

  def encode_repeated(%Field{is_packable: false} = field, list, schema_ctx, msg_desc)
      when is_list(list) do
    Enum.map(list, fn item ->
      encode_field_value(field, item, schema_ctx, msg_desc)
    end)
  end

  def encode_repeated(field, list, schema_ctx, msg_desc) when is_list(list) do
    is_packable =
      if is_boolean(field.is_packable),
        do: field.is_packable,
        else: is_packable_type?(field.type, schema_ctx, msg_desc)

    if is_packable do
      packed_bytes =
        Enum.map(list, fn item ->
          encode_raw_value(field.type, item, schema_ctx, msg_desc)
        end)

      tag = field.tag_bytes || encode_tag(field.number, 2)
      [tag, encode_varint(IO.iodata_length(packed_bytes)), packed_bytes]
    else
      Enum.map(list, fn item ->
        encode_field_value(field, item, schema_ctx, msg_desc)
      end)
    end
  end

  @doc false
  def encode_map_entries(field, map, schema_ctx, msg_desc) when is_map(map) do
    Enum.map(map, fn {k, v} ->
      k_bytes = encode_single_field(1, field.key_type, k, schema_ctx, msg_desc)
      v_bytes = encode_map_value(field, v, schema_ctx, msg_desc)
      entry_len = IO.iodata_length(k_bytes) + IO.iodata_length(v_bytes)
      tag = field.tag_bytes || encode_tag(field.number, 2)
      [tag, encode_varint(entry_len), k_bytes, v_bytes]
    end)
  end

  # A map's message value is named by `submessage_desc` (a stub that carries only the
  # name: the value type may refer back to the message holding the map) and its enum value
  # by `enum_desc`, so a codec can encode and decode entries without resolving the value's
  # type by a name that is relative to the map's scope. The compiled codec passes only
  # itself as the schema context.
  defp encode_map_value(
         %Field{submessage_desc: %Message{full_name: name}},
         v,
         schema_ctx,
         _msg_desc
       )
       when is_binary(name) and (is_map(v) or is_list(v)) do
    payload = encode!(name, v, schema_ctx)
    [encode_tag(2, 2), encode_varint(byte_size(payload)), payload]
  end

  defp encode_map_value(%Field{enum_desc: %Schema.Enum{} = enum_desc}, v, _schema_ctx, _msg_desc) do
    [encode_tag(2, 0), encode_varint(resolve_enum_value_to_integer(v, enum_desc))]
  end

  defp encode_map_value(field, v, schema_ctx, msg_desc) do
    encode_single_field(2, field.val_type, v, schema_ctx, msg_desc)
  end

  @doc false
  def decode_map_entry(binary, %Field{} = field, schema_ctx \\ nil) do
    decode_field_value(binary, 2, field, schema_ctx, nil)
  end

  @doc false
  def encode_single_field(
        field_num,
        type,
        value,
        schema_ctx,
        msg_desc,
        tag_bytes \\ nil,
        wire_type \\ nil
      ) do
    wire_type = wire_type || wire_type_for(type, schema_ctx, msg_desc)
    tag = tag_bytes || encode_tag(field_num, wire_type)

    case wire_type do
      0 ->
        [tag, encode_varint_type(type, value, schema_ctx, msg_desc)]

      1 ->
        [tag, encode_64bit(type, value)]

      2 ->
        payload = encode_length_delimited_payload(type, value, schema_ctx)
        [tag, encode_varint(IO.iodata_length(payload)), payload]

      5 ->
        [tag, encode_32bit(type, value)]
    end
  end

  defp encode_raw_value(type, value, schema_ctx, msg_desc) do
    case wire_type_for(type, schema_ctx, msg_desc) do
      0 ->
        encode_varint_type(type, value, schema_ctx, msg_desc)

      1 ->
        encode_64bit(type, value)

      5 ->
        encode_32bit(type, value)

      2 ->
        payload = encode_length_delimited_payload(type, value, schema_ctx)
        [encode_varint(IO.iodata_length(payload)), payload]
    end
  end

  defp encode_length_delimited_payload("string", value, _schema_ctx) when is_binary(value),
    do: value

  defp encode_length_delimited_payload("string", value, _schema_ctx) when is_atom(value),
    do: Atom.to_string(value)

  defp encode_length_delimited_payload("bytes", value, _schema_ctx) when is_binary(value),
    do: value

  defp encode_length_delimited_payload({:map, key_type, val_type}, value, schema_ctx)
       when is_map(value) do
    Enum.map(value, fn {k, v} ->
      k_bytes = encode_single_field(1, key_type, k, schema_ctx, nil)
      v_bytes = encode_single_field(2, val_type, v, schema_ctx, nil)
      entry_len = IO.iodata_length(k_bytes) + IO.iodata_length(v_bytes)
      tag = encode_tag(1, 2)
      [tag, encode_varint(entry_len), k_bytes, v_bytes]
    end)
  end

  defp encode_length_delimited_payload(type, value, %Schema{compiled_module: mod})
       when not is_nil(mod) and is_binary(type) do
    case mod.encode(type, value) do
      :error -> throw({:error, "Cannot resolve nested message #{inspect(type)}"})
      iodata -> iodata
    end
  end

  defp encode_length_delimited_payload(type, value, schema_ctx) do
    # Embedded sub-message
    sub_desc = resolve_submessage_descriptor(type, schema_ctx)
    encode_fields(sub_desc.fields, value, schema_ctx, sub_desc)
  end

  @doc false
  def encode_tag(field_num, wire_type) do
    encode_varint(field_num <<< 3 ||| wire_type)
  end

  defp encode_varint_type("bool", true, _schema_ctx, _msg_desc), do: <<1>>
  defp encode_varint_type("bool", false, _schema_ctx, _msg_desc), do: <<0>>

  defp encode_varint_type("sint32", val, _schema_ctx, _msg_desc) when is_integer(val),
    do: encode_varint(encode_zigzag32(val))

  defp encode_varint_type("sint64", val, _schema_ctx, _msg_desc) when is_integer(val),
    do: encode_varint(encode_zigzag64(val))

  defp encode_varint_type(_type, val, _schema_ctx, _msg_desc) when is_integer(val),
    do: encode_varint(val)

  defp encode_varint_type(type, val, schema_ctx, msg_desc) when is_atom(val) or is_binary(val) do
    enum_desc = resolve_enum_descriptor(type, schema_ctx, msg_desc)

    if enum_desc do
      int_val = resolve_enum_value_to_integer(val, enum_desc)
      encode_varint(int_val)
    else
      throw(
        {:error,
         "Cannot resolve enum descriptor for type #{inspect(type)} and value #{inspect(val)}"}
      )
    end
  end

  @doc false
  def resolve_enum_value_to_integer(val, %Schema.Enum{
        atom_to_int: a2i,
        str_to_int: s2i,
        fields: fields,
        name: name
      }) do
    cond do
      is_integer(val) ->
        val

      is_atom(val) ->
        case Map.get(a2i, val) do
          nil ->
            val_str = Atom.to_string(val)

            case Map.get(s2i, val_str) || Map.get(s2i, String.upcase(val_str)) do
              nil ->
                case Enum.find(fields, fn {k, _v} ->
                       k == val or Atom.to_string(k) == val_str or
                         String.upcase(Atom.to_string(k)) == String.upcase(val_str)
                     end) do
                  {_, int_val} ->
                    int_val

                  nil ->
                    throw({:error, "Unknown enum atom #{inspect(val)} for enum #{inspect(name)}"})
                end

              int_val ->
                int_val
            end

          int_val ->
            int_val
        end

      is_binary(val) ->
        case Map.get(s2i, val) || Map.get(s2i, String.upcase(val)) do
          nil ->
            case Integer.parse(val) do
              {int_val, ""} ->
                int_val

              _ ->
                case Enum.find(fields, fn {k, _v} ->
                       Atom.to_string(k) == val or
                         String.upcase(Atom.to_string(k)) == String.upcase(val)
                     end) do
                  {_, int_val} ->
                    int_val

                  nil ->
                    throw(
                      {:error, "Unknown enum string #{inspect(val)} for enum #{inspect(name)}"}
                    )
                end
            end

          int_val ->
            int_val
        end

      true ->
        throw({:error, "Invalid enum value #{inspect(val)} for enum #{inspect(name)}"})
    end
  end

  @doc false
  def resolve_enum_integer_to_atom(int_val, %Schema.Enum{int_to_atom: i2a, fields: fields})
      when is_integer(int_val) do
    signed_32 =
      cond do
        int_val >= 0x8000000000000000 -> int_val - 0x10000000000000000
        int_val >= 0x80000000 and int_val <= 0xFFFFFFFF -> int_val - 0x100000000
        true -> int_val
      end

    case Map.get(i2a, int_val) || Map.get(i2a, signed_32) do
      nil ->
        case Enum.find(fields, fn {_k, v} -> v == int_val or v == signed_32 end) do
          {atom_name, _} -> atom_name
          nil -> signed_32
        end

      atom_name ->
        atom_name
    end
  end

  def resolve_enum_integer_to_atom(int_val, _other), do: int_val

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

  defp decode_wire_fields(binary, %Message{fields_by_number: fbn} = msg_desc, schema_ctx, acc) do
    {tag, rest} = decode_varint(binary)
    field_num = tag >>> 3
    wire_type = tag &&& 0x07

    case Map.get(fbn, field_num) do
      nil ->
        orig_len = byte_size(binary)
        {_skipped, remaining} = skip_wire_value(rest, wire_type)
        consumed_len = orig_len - byte_size(remaining)
        unknown_wire = :binary.part(binary, 0, consumed_len)
        unknowns = Map.get(acc, :__unknown_fields__, []) ++ [unknown_wire]

        decode_wire_fields(
          remaining,
          msg_desc,
          schema_ctx,
          Map.put(acc, :__unknown_fields__, unknowns)
        )

      field_desc ->
        {field_value, remaining} =
          decode_field_value(rest, wire_type, field_desc, schema_ctx, msg_desc)

        updated_acc = store_decoded_field(acc, field_desc, field_value, msg_desc)
        decode_wire_fields(remaining, msg_desc, schema_ctx, updated_acc)
    end
  end

  # An entry without a value means the value type's default.
  defp map_value_type(%Field{submessage_desc: %Message{full_name: name}}) when is_binary(name),
    do: name

  defp map_value_type(%Field{val_type: val_type}), do: val_type

  defp map_value_default(
         %Field{submessage_desc: %Message{full_name: name}},
         schema_ctx,
         _entry_desc
       )
       when is_binary(name),
       do: decode!(name, <<>>, schema_ctx)

  defp map_value_default(%Field{enum_desc: %Schema.Enum{} = enum_desc}, _schema_ctx, _entry_desc),
    do: enum_desc.zero_atom || 0

  defp map_value_default(field, schema_ctx, entry_desc),
    do: default_scalar_value(field.val_type, schema_ctx, entry_desc)

  defp decode_field_value(binary, 2, %Field{kind: :map} = field, schema_ctx, _msg_desc) do
    {len, rest} = decode_varint(binary)
    <<map_entry_bytes::binary-size(^len), remaining::binary>> = rest

    # Map entry is a sub-message with field 1 (key) and field 2 (value)
    key_field = %Field{name: :key, number: 1, type: field.key_type, kind: :field}

    value_field = %Field{
      name: :value,
      number: 2,
      type: map_value_type(field),
      kind: :field,
      enum_desc: field.enum_desc
    }

    entry_desc = %Message{
      name: "MapEntry",
      fields: [key_field, value_field],
      fields_by_number: %{1 => key_field, 2 => value_field}
    }

    decoded_entry = decode_wire_fields(map_entry_bytes, entry_desc, schema_ctx, %{})
    k = Map.get(decoded_entry, :key, default_scalar_value(field.key_type, schema_ctx, entry_desc))

    v =
      case Map.fetch(decoded_entry, :value) do
        {:ok, value} -> value
        :error -> map_value_default(field, schema_ctx, entry_desc)
      end

    {{k, v}, remaining}
  end

  # Repeated Submessage (Hot Path)
  defp decode_field_value(
         binary,
         2,
         %Field{prefix: :repeated, submessage_desc: %Message{} = sub_desc},
         schema_ctx,
         _msg_desc
       ) do
    {len, rest} = decode_varint(binary)
    <<sub_bytes::binary-size(^len), remaining::binary>> = rest
    sub_decoded = decode!(sub_desc, sub_bytes, schema_ctx)
    {[sub_decoded], remaining}
  end

  # Non-repeated Submessage (Hot Path)
  defp decode_field_value(
         binary,
         2,
         %Field{submessage_desc: %Message{} = sub_desc},
         schema_ctx,
         _msg_desc
       ) do
    {len, rest} = decode_varint(binary)
    <<sub_bytes::binary-size(^len), remaining::binary>> = rest
    sub_decoded = decode!(sub_desc, sub_bytes, schema_ctx)
    {sub_decoded, remaining}
  end

  # Repeated String
  defp decode_field_value(
         binary,
         2,
         %Field{prefix: :repeated, type: "string"},
         _schema_ctx,
         _msg_desc
       ) do
    {len, rest} = decode_varint(binary)
    <<str::binary-size(^len), remaining::binary>> = rest
    {[str], remaining}
  end

  # Non-repeated String
  defp decode_field_value(binary, 2, %Field{type: "string"}, _schema_ctx, _msg_desc) do
    {len, rest} = decode_varint(binary)
    <<str::binary-size(^len), remaining::binary>> = rest
    {str, remaining}
  end

  # Repeated Bytes
  defp decode_field_value(
         binary,
         2,
         %Field{prefix: :repeated, type: "bytes"},
         _schema_ctx,
         _msg_desc
       ) do
    {len, rest} = decode_varint(binary)
    <<b::binary-size(^len), remaining::binary>> = rest
    {[b], remaining}
  end

  # Non-repeated Bytes
  defp decode_field_value(binary, 2, %Field{type: "bytes"}, _schema_ctx, _msg_desc) do
    {len, rest} = decode_varint(binary)
    <<b::binary-size(^len), remaining::binary>> = rest
    {b, remaining}
  end

  # Enum field (wire type 0)
  defp decode_field_value(
         binary,
         0,
         %Field{enum_desc: %Schema.Enum{} = enum_desc, prefix: :repeated},
         _schema_ctx,
         _msg_desc
       ) do
    {int_val, rest} = decode_varint(binary)
    atom_val = resolve_enum_integer_to_atom(int_val, enum_desc)
    {[atom_val], rest}
  end

  defp decode_field_value(
         binary,
         0,
         %Field{enum_desc: %Schema.Enum{} = enum_desc},
         _schema_ctx,
         _msg_desc
       ) do
    {int_val, rest} = decode_varint(binary)
    atom_val = resolve_enum_integer_to_atom(int_val, enum_desc)
    {atom_val, rest}
  end

  # Packed repeated field (wire type 2)
  defp decode_field_value(
         binary,
         2,
         %Field{prefix: :repeated, is_packable: true} = field,
         schema_ctx,
         msg_desc
       ) do
    {len, rest} = decode_varint(binary)
    <<packed_bytes::binary-size(^len), remaining::binary>> = rest
    items = decode_packed_values(packed_bytes, field.type, schema_ctx, msg_desc)
    {items, remaining}
  end

  # Double
  defp decode_field_value(
         <<val::float-little-size(64), rest::binary>>,
         1,
         %Field{type: "double", prefix: :repeated},
         _schema_ctx,
         _msg_desc
       ) do
    {[val], rest}
  end

  defp decode_field_value(
         <<val::float-little-size(64), rest::binary>>,
         1,
         %Field{type: "double"},
         _schema_ctx,
         _msg_desc
       ) do
    {val, rest}
  end

  # Float
  defp decode_field_value(
         <<val::float-little-size(32), rest::binary>>,
         5,
         %Field{type: "float", prefix: :repeated},
         _schema_ctx,
         _msg_desc
       ) do
    {[val], rest}
  end

  defp decode_field_value(
         <<val::float-little-size(32), rest::binary>>,
         5,
         %Field{type: "float"},
         _schema_ctx,
         _msg_desc
       ) do
    {val, rest}
  end

  # Fixed64 / SFixed64
  defp decode_field_value(
         <<val::little-unsigned-size(64), rest::binary>>,
         1,
         %Field{type: "fixed64"},
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_field_value(
         <<val::little-signed-size(64), rest::binary>>,
         1,
         %Field{type: "sfixed64"},
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  # Fixed32 / SFixed32
  defp decode_field_value(
         <<val::little-unsigned-size(32), rest::binary>>,
         5,
         %Field{type: "fixed32"},
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_field_value(
         <<val::little-signed-size(32), rest::binary>>,
         5,
         %Field{type: "sfixed32"},
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  # Varint integers & bools
  defp decode_field_value(binary, 0, %Field{type: t, prefix: :repeated}, _schema_ctx, _msg_desc)
       when t in ["int32", "int64", "uint32", "uint64"] do
    {val, rest} = decode_varint(binary)
    {[val], rest}
  end

  defp decode_field_value(binary, 0, %Field{type: t}, _schema_ctx, _msg_desc)
       when t in ["int32", "int64", "uint32", "uint64"] do
    decode_varint(binary)
  end

  defp decode_field_value(
         binary,
         0,
         %Field{type: "bool", prefix: :repeated},
         _schema_ctx,
         _msg_desc
       ) do
    {val, rest} = decode_varint(binary)
    {[val != 0], rest}
  end

  defp decode_field_value(binary, 0, %Field{type: "bool"}, _schema_ctx, _msg_desc) do
    {val, rest} = decode_varint(binary)
    {val != 0, rest}
  end

  defp decode_field_value(binary, 0, %Field{type: "sint32"}, _schema_ctx, _msg_desc) do
    {val, rest} = decode_varint(binary)
    {decode_zigzag32(val), rest}
  end

  defp decode_field_value(binary, 0, %Field{type: "sint64"}, _schema_ctx, _msg_desc) do
    {val, rest} = decode_varint(binary)
    {decode_zigzag64(val), rest}
  end

  defp decode_field_value(binary, 2, %Field{prefix: :repeated} = field, schema_ctx, msg_desc) do
    # Fallback for dynamic/unknown repeated types
    if is_packable_type?(field.type, schema_ctx, msg_desc) do
      {len, rest} = decode_varint(binary)
      <<packed_bytes::binary-size(^len), remaining::binary>> = rest
      items = decode_packed_values(packed_bytes, field.type, schema_ctx, msg_desc)
      {items, remaining}
    else
      {item, remaining} = decode_scalar_or_message(binary, 2, field.type, schema_ctx, msg_desc)
      {[item], remaining}
    end
  end

  defp decode_field_value(binary, 0, %Field{prefix: :repeated} = field, schema_ctx, msg_desc) do
    {item, remaining} = decode_scalar_or_message(binary, 0, field.type, schema_ctx, msg_desc)
    {[item], remaining}
  end

  defp decode_field_value(binary, wire_type, field, schema_ctx, msg_desc) do
    decode_scalar_or_message(binary, wire_type, field.type, schema_ctx, msg_desc)
  end

  defp decode_scalar_or_message(binary, 0, "int32", _schema_ctx, _msg_desc) do
    decode_varint(binary)
  end

  defp decode_scalar_or_message(binary, 0, "int64", _schema_ctx, _msg_desc) do
    decode_varint(binary)
  end

  defp decode_scalar_or_message(binary, 0, "uint32", _schema_ctx, _msg_desc) do
    decode_varint(binary)
  end

  defp decode_scalar_or_message(binary, 0, "uint64", _schema_ctx, _msg_desc) do
    decode_varint(binary)
  end

  defp decode_scalar_or_message(binary, 0, "bool", _schema_ctx, _msg_desc) do
    {val, rest} = decode_varint(binary)
    {val != 0, rest}
  end

  defp decode_scalar_or_message(binary, 0, "sint32", _schema_ctx, _msg_desc) do
    {val, rest} = decode_varint(binary)
    {decode_zigzag32(val), rest}
  end

  defp decode_scalar_or_message(binary, 0, "sint64", _schema_ctx, _msg_desc) do
    {val, rest} = decode_varint(binary)
    {decode_zigzag64(val), rest}
  end

  defp decode_scalar_or_message(binary, 0, type, schema_ctx, msg_desc) do
    {val, rest} = decode_varint(binary)

    if is_enum_type?(type, schema_ctx, msg_desc) do
      enum_desc = resolve_enum_descriptor(type, schema_ctx, msg_desc)
      {resolve_enum_integer_to_atom(val, enum_desc), rest}
    else
      {val, rest}
    end
  end

  defp decode_scalar_or_message(
         <<val::little-unsigned-size(64), rest::binary>>,
         1,
         "fixed64",
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_scalar_or_message(
         <<val::little-signed-size(64), rest::binary>>,
         1,
         "sfixed64",
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_scalar_or_message(
         <<val::float-little-size(64), rest::binary>>,
         1,
         "double",
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_scalar_or_message(
         <<val::little-unsigned-size(64), rest::binary>>,
         1,
         _type,
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_scalar_or_message(
         <<val::little-unsigned-size(32), rest::binary>>,
         5,
         "fixed32",
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_scalar_or_message(
         <<val::little-signed-size(32), rest::binary>>,
         5,
         "sfixed32",
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_scalar_or_message(
         <<val::float-little-size(32), rest::binary>>,
         5,
         "float",
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_scalar_or_message(
         <<val::little-unsigned-size(32), rest::binary>>,
         5,
         _type,
         _schema_ctx,
         _msg_desc
       ),
       do: {val, rest}

  defp decode_scalar_or_message(binary, 2, "string", _schema_ctx, _msg_desc) do
    {len, rest} = decode_varint(binary)
    <<str::binary-size(^len), remaining::binary>> = rest
    {str, remaining}
  end

  defp decode_scalar_or_message(binary, 2, "bytes", _schema_ctx, _msg_desc) do
    {len, rest} = decode_varint(binary)
    <<bytes::binary-size(^len), remaining::binary>> = rest
    {bytes, remaining}
  end

  defp decode_scalar_or_message(binary, 2, type, %Schema{compiled_module: mod}, _msg_desc)
       when not is_nil(mod) and is_binary(type) do
    {len, rest} = decode_varint(binary)
    <<sub_bytes::binary-size(^len), remaining::binary>> = rest

    case mod.decode(type, sub_bytes) do
      :error -> throw({:error, "Cannot resolve nested message #{inspect(type)}"})
      sub_decoded when is_map(sub_decoded) -> {sub_decoded, remaining}
    end
  end

  defp decode_scalar_or_message(binary, 2, type, schema_ctx, _msg_desc) do
    # Embedded submessage
    {len, rest} = decode_varint(binary)
    <<sub_bytes::binary-size(^len), remaining::binary>> = rest
    sub_desc = resolve_submessage_descriptor(type, schema_ctx)
    sub_decoded = decode!(sub_desc, sub_bytes, schema_ctx)
    {sub_decoded, remaining}
  end

  @doc false
  def decode_packed_values(binary, type, schema_ctx \\ nil, msg_desc \\ nil)

  def decode_packed_values(<<>>, _type, _schema_ctx, _msg_desc), do: []

  def decode_packed_values(binary, "int32", _schema_ctx, _msg_desc) do
    decode_packed_varints(binary)
  end

  def decode_packed_values(binary, "int64", _schema_ctx, _msg_desc) do
    decode_packed_varints(binary)
  end

  def decode_packed_values(binary, "uint32", _schema_ctx, _msg_desc) do
    decode_packed_varints(binary)
  end

  def decode_packed_values(binary, "uint64", _schema_ctx, _msg_desc) do
    decode_packed_varints(binary)
  end

  def decode_packed_values(binary, "double", _schema_ctx, _msg_desc) do
    decode_packed_doubles(binary)
  end

  def decode_packed_values(binary, "float", _schema_ctx, _msg_desc) do
    decode_packed_floats(binary)
  end

  def decode_packed_values(binary, type, schema_ctx, msg_desc) do
    wire_type = wire_type_for(type, schema_ctx, msg_desc)
    {item, rest} = decode_scalar_or_message(binary, wire_type, type, schema_ctx, msg_desc)
    [item | decode_packed_values(rest, type, schema_ctx, msg_desc)]
  end

  @doc false
  # A packed run of enum varints, decoded with the field's own enum descriptor.
  def decode_packed_enums(<<>>, _enum_desc), do: []

  def decode_packed_enums(binary, %Schema.Enum{} = enum_desc) do
    {val, rest} = decode_varint(binary)
    [resolve_enum_integer_to_atom(val, enum_desc) | decode_packed_enums(rest, enum_desc)]
  end

  defp decode_packed_varints(<<>>), do: []

  defp decode_packed_varints(binary) do
    {val, rest} = decode_varint(binary)
    [val | decode_packed_varints(rest)]
  end

  defp decode_packed_doubles(<<>>), do: []

  defp decode_packed_doubles(<<val::float-little-size(64), rest::binary>>) do
    [val | decode_packed_doubles(rest)]
  end

  defp decode_packed_floats(<<>>), do: []

  defp decode_packed_floats(<<val::float-little-size(32), rest::binary>>) do
    [val | decode_packed_floats(rest)]
  end

  defp store_decoded_field(acc, %Field{oneof_siblings: siblings} = field, value, _msg_desc)
       when is_list(siblings) and siblings != [] do
    acc
    |> Map.drop(siblings)
    |> Map.put(field.name, value)
  end

  defp store_decoded_field(acc, %Field{kind: :map} = field, {k, v}, _msg_desc) do
    existing_map = Map.get(acc, field.name, %{})
    Map.put(acc, field.name, Map.put(existing_map, k, v))
  end

  defp store_decoded_field(acc, %Field{prefix: :repeated} = field, [single_item], _msg_desc) do
    existing_list = Map.get(acc, field.name, [])
    Map.put(acc, field.name, [single_item | existing_list])
  end

  defp store_decoded_field(acc, %Field{prefix: :repeated} = field, items, _msg_desc)
       when is_list(items) do
    existing_list = Map.get(acc, field.name, [])
    reversed_items = Enum.reverse(items)
    Map.put(acc, field.name, reversed_items ++ existing_list)
  end

  defp store_decoded_field(acc, field, value, %Message{} = msg_desc) do
    case find_oneof_for_field(msg_desc, field.name) do
      %Field{fields: subfields} ->
        sibling_names = Enum.map(subfields, & &1.name)

        acc
        |> Map.drop(sibling_names)
        |> Map.put(field.name, value)

      _ ->
        Map.put(acc, field.name, value)
    end
  end

  defp find_oneof_for_field(%Message{fields: fields}, field_name) do
    Enum.find(fields, fn
      %Field{kind: :oneof, fields: subfields} ->
        Enum.any?(subfields, &(&1.name == field_name))

      _ ->
        false
    end)
  end

  @doc false
  def skip_wire_value(binary, 0) do
    {_val, rest} = decode_varint(binary)
    {:ok, rest}
  end

  def skip_wire_value(<<_::64, rest::binary>>, 1), do: {:ok, rest}

  def skip_wire_value(binary, 2) do
    {len, rest} = decode_varint(binary)
    <<_::binary-size(^len), remaining::binary>> = rest
    {:ok, remaining}
  end

  def skip_wire_value(binary, 3) do
    skip_group(binary)
  end

  def skip_wire_value(<<_::32, rest::binary>>, 5), do: {:ok, rest}
  def skip_wire_value(_binary, _wire_type), do: {:error, :invalid_wire_type}

  defp skip_group(binary) do
    {tag, rest} = decode_varint(binary)
    wire_type = tag &&& 0x07

    case wire_type do
      4 ->
        {:ok, rest}

      _ ->
        {:ok, remaining} = skip_wire_value(rest, wire_type)
        skip_group(remaining)
    end
  end

  # ---------------------------------------------------------------------------
  # Default Values & Coercion
  # ---------------------------------------------------------------------------

  defp apply_defaults_and_coerce(%Message{fields: fields} = msg_desc, raw_map, schema_ctx) do
    Enum.reduce(fields, %{}, fn field, acc ->
      case field.kind do
        :oneof ->
          Enum.reduce(field.fields, acc, fn oneof_subfield, inner_acc ->
            if Map.has_key?(raw_map, oneof_subfield.name) do
              Map.put(inner_acc, oneof_subfield.name, Map.get(raw_map, oneof_subfield.name))
            else
              Map.put(inner_acc, oneof_subfield.name, nil)
            end
          end)

        :map ->
          val = Map.get(raw_map, field.name, %{})
          Map.put(acc, field.name, val)

        _ ->
          if field.prefix == :repeated do
            case Map.get(raw_map, field.name) do
              nil -> Map.put(acc, field.name, [])
              val -> Map.put(acc, field.name, Enum.reverse(val))
            end
          else
            if field.prefix == :optional do
              val = Map.get(raw_map, field.name, nil)
              Map.put(acc, field.name, val)
            else
              default_val =
                if field.default_value != nil do
                  field.default_value
                else
                  default_scalar_value(field.type, schema_ctx, msg_desc)
                end

              val = Map.get(raw_map, field.name, default_val)
              Map.put(acc, field.name, val)
            end
          end
      end
    end)
  end

  defp default_scalar_value("string", _schema_ctx, _msg_desc), do: ""
  defp default_scalar_value("bytes", _schema_ctx, _msg_desc), do: <<>>
  defp default_scalar_value("bool", _schema_ctx, _msg_desc), do: false
  defp default_scalar_value("int32", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("int64", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("uint32", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("uint64", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("sint32", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("sint64", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("fixed32", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("fixed64", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("sfixed32", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("sfixed64", _schema_ctx, _msg_desc), do: 0
  defp default_scalar_value("float", _schema_ctx, _msg_desc), do: 0.0
  defp default_scalar_value("double", _schema_ctx, _msg_desc), do: 0.0

  defp default_scalar_value(type, schema_ctx, msg_desc) do
    if is_enum_type?(type, schema_ctx, msg_desc) do
      enum_desc = resolve_enum_descriptor(type, schema_ctx, msg_desc)

      if enum_desc do
        enum_desc.zero_atom ||
          Enum.find_value(enum_desc.fields, 0, fn {k, v} -> if v == 0, do: k end)
      else
        0
      end
    else
      nil
    end
  end

  defp is_default_value?(val, "string", _schema_ctx, _msg_desc), do: val == ""
  defp is_default_value?(val, "bytes", _schema_ctx, _msg_desc), do: val == <<>>
  defp is_default_value?(val, "bool", _schema_ctx, _msg_desc), do: val == false
  defp is_default_value?(val, "int32", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "int64", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "uint32", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "uint64", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "sint32", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "sint64", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "fixed32", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "fixed64", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "sfixed32", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "sfixed64", _schema_ctx, _msg_desc), do: val == 0
  defp is_default_value?(val, "float", _schema_ctx, _msg_desc), do: val == 0.0 or val == 0
  defp is_default_value?(val, "double", _schema_ctx, _msg_desc), do: val == 0.0 or val == 0

  defp is_default_value?(val, type, schema_ctx, msg_desc) do
    if is_enum_type?(type, schema_ctx, msg_desc) do
      enum_desc = resolve_enum_descriptor(type, schema_ctx, msg_desc)

      if enum_desc do
        zero_atom =
          enum_desc.zero_atom ||
            Enum.find_value(enum_desc.fields, fn {k, v} -> if v == 0, do: k end)

        val == 0 or val == zero_atom or
          (is_atom(val) and zero_atom != nil and
             String.upcase(Atom.to_string(val)) == String.upcase(Atom.to_string(zero_atom))) or
          (is_binary(val) and zero_atom != nil and
             String.upcase(val) == String.upcase(Atom.to_string(zero_atom))) or
          (is_binary(val) and val == "0")
      else
        val == 0
      end
    else
      false
    end
  end

  # ---------------------------------------------------------------------------
  # Varint & Bitwise Utilities
  # ---------------------------------------------------------------------------

  @doc """
  Encodes an integer into standard protobuf varint bytes.
  """
  def encode_varint(n) when is_integer(n) and n >= 0 and n < 128, do: <<n::8>>

  def encode_varint(n) when is_integer(n) and n >= 0 and n < 16384 do
    <<1::1, n &&& 0x7F::7, 0::1, n >>> 7::7>>
  end

  def encode_varint(n) when is_integer(n) and n >= 0 and n < 2_097_152 do
    <<1::1, n &&& 0x7F::7, 1::1, n >>> 7 &&& 0x7F::7, 0::1, n >>> 14::7>>
  end

  def encode_varint(n) when is_integer(n) and n >= 0 and n < 268_435_456 do
    <<1::1, n &&& 0x7F::7, 1::1, n >>> 7 &&& 0x7F::7, 1::1, n >>> 14 &&& 0x7F::7, 0::1,
      n >>> 21::7>>
  end

  def encode_varint(n) when is_integer(n) and n >= 0 do
    <<1::1, n &&& 0x7F::7, encode_varint(n >>> 7)::binary>>
  end

  def encode_varint(n) when is_integer(n) and n < 0 do
    encode_varint(n &&& 0xFFFFFFFFFFFFFFFF)
  end

  @doc """
  Decodes varint bytes into `{integer, remaining_binary}`.
  """
  def decode_varint(<<0::1, val::7, rest::binary>>), do: {val, rest}

  def decode_varint(<<1::1, v1::7, 0::1, v2::7, rest::binary>>) do
    {v1 ||| v2 <<< 7, rest}
  end

  def decode_varint(<<1::1, v1::7, 1::1, v2::7, 0::1, v3::7, rest::binary>>) do
    {v1 ||| v2 <<< 7 ||| v3 <<< 14, rest}
  end

  def decode_varint(<<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 0::1, v4::7, rest::binary>>) do
    {v1 ||| v2 <<< 7 ||| v3 <<< 14 ||| v4 <<< 21, rest}
  end

  def decode_varint(binary, acc \\ 0, shift \\ 0)

  def decode_varint(<<0::1, val::7, rest::binary>>, acc, shift) do
    {acc ||| val <<< shift, rest}
  end

  def decode_varint(<<1::1, val::7, rest::binary>>, acc, shift) do
    decode_varint(rest, acc ||| val <<< shift, shift + 7)
  end

  def decode_varint(<<>>, _acc, _shift) do
    throw({:error, "Unexpected EOF while decoding varint"})
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

  def wire_type_for("int32", _s, _m), do: 0
  def wire_type_for("int64", _s, _m), do: 0
  def wire_type_for("uint32", _s, _m), do: 0
  def wire_type_for("uint64", _s, _m), do: 0
  def wire_type_for("sint32", _s, _m), do: 0
  def wire_type_for("sint64", _s, _m), do: 0
  def wire_type_for("bool", _s, _m), do: 0
  def wire_type_for("enum", _s, _m), do: 0
  def wire_type_for("fixed64", _s, _m), do: 1
  def wire_type_for("sfixed64", _s, _m), do: 1
  def wire_type_for("double", _s, _m), do: 1
  def wire_type_for("string", _s, _m), do: 2
  def wire_type_for("bytes", _s, _m), do: 2
  def wire_type_for("fixed32", _s, _m), do: 5
  def wire_type_for("sfixed32", _s, _m), do: 5
  def wire_type_for("float", _s, _m), do: 5

  def wire_type_for(type, schema_ctx, msg_desc) do
    if is_tuple(type) do
      2
    else
      if is_enum_type?(type, schema_ctx, msg_desc) do
        0
      else
        2
      end
    end
  end

  def is_packable_type?("int32", _s, _m), do: true
  def is_packable_type?("int64", _s, _m), do: true
  def is_packable_type?("uint32", _s, _m), do: true
  def is_packable_type?("uint64", _s, _m), do: true
  def is_packable_type?("sint32", _s, _m), do: true
  def is_packable_type?("sint64", _s, _m), do: true
  def is_packable_type?("bool", _s, _m), do: true
  def is_packable_type?("enum", _s, _m), do: true
  def is_packable_type?("fixed64", _s, _m), do: true
  def is_packable_type?("sfixed64", _s, _m), do: true
  def is_packable_type?("double", _s, _m), do: true
  def is_packable_type?("fixed32", _s, _m), do: true
  def is_packable_type?("sfixed32", _s, _m), do: true
  def is_packable_type?("float", _s, _m), do: true
  def is_packable_type?("string", _s, _m), do: false
  def is_packable_type?("bytes", _s, _m), do: false

  def is_packable_type?(type, schema_ctx, msg_desc) do
    is_enum_type?(type, schema_ctx, msg_desc)
  end

  def is_enum_type?("enum", _s, _m), do: true
  def is_enum_type?("int32", _s, _m), do: false
  def is_enum_type?("int64", _s, _m), do: false
  def is_enum_type?("uint32", _s, _m), do: false
  def is_enum_type?("uint64", _s, _m), do: false
  def is_enum_type?("sint32", _s, _m), do: false
  def is_enum_type?("sint64", _s, _m), do: false
  def is_enum_type?("bool", _s, _m), do: false
  def is_enum_type?("fixed64", _s, _m), do: false
  def is_enum_type?("sfixed64", _s, _m), do: false
  def is_enum_type?("double", _s, _m), do: false
  def is_enum_type?("fixed32", _s, _m), do: false
  def is_enum_type?("sfixed32", _s, _m), do: false
  def is_enum_type?("float", _s, _m), do: false
  def is_enum_type?("string", _s, _m), do: false
  def is_enum_type?("bytes", _s, _m), do: false

  def is_enum_type?(type, schema_ctx, msg_desc) do
    not is_nil(resolve_enum_descriptor(type, schema_ctx, msg_desc))
  end

  # ---------------------------------------------------------------------------
  # Descriptor & Data Resolvers
  # ---------------------------------------------------------------------------

  def resolve_enum_descriptor(type_name, schema_ctx, msg_desc \\ nil)

  def resolve_enum_descriptor({:enum, type_name}, schema_ctx, msg_desc) do
    resolve_enum_descriptor(type_name, schema_ctx, msg_desc)
  end

  def resolve_enum_descriptor(type_name, %Schema{} = schema, msg_desc) do
    scope = msg_desc && Map.get(msg_desc, :full_name)

    Schema.resolve_enum(schema, type_name, scope) ||
      (msg_desc && find_in_msg_nested_enums(msg_desc, type_name))
  end

  def resolve_enum_descriptor(type_name, %{enums: enums} = _map_schema, msg_desc) do
    type_str = type_name |> to_string() |> String.trim_leading(".")

    (msg_desc && find_in_msg_nested_enums(msg_desc, type_name)) ||
      Map.get(enums, type_name) ||
      Map.get(enums, type_str) ||
      (is_binary(type_name) && Map.get(enums, String.to_atom(type_str))) ||
      Enum.find_value(enums, fn {_k, e} ->
        if is_struct(e, Schema.Enum) and (e.name == type_str or e.name == type_name), do: e
      end)
  end

  def resolve_enum_descriptor(type_name, mod, msg_desc) when is_atom(mod) and not is_nil(mod) do
    (msg_desc && find_in_msg_nested_enums(msg_desc, type_name)) ||
      if function_exported?(mod, :__schema__, 0) do
        resolve_enum_descriptor(type_name, mod.__schema__(), msg_desc)
      else
        nil
      end
  end

  def resolve_enum_descriptor(type_name, nil, %Message{} = msg_desc) do
    find_in_msg_nested_enums(msg_desc, type_name)
  end

  def resolve_enum_descriptor(_type_name, _schema_ctx, _msg_desc), do: nil

  defp find_in_msg_nested_enums(%Message{nested_enums: nested_enums, name: msg_name}, type_name) do
    type_str = type_name |> to_string() |> String.trim_leading(".")

    Enum.find(nested_enums, fn
      %Schema.Enum{name: name} ->
        name == type_str or "#{msg_name}.#{name}" == type_str or
          String.ends_with?(type_str, ".#{name}")

      _ ->
        false
    end)
  end

  defp find_in_msg_nested_enums(_other, _type_name), do: nil

  defp extract_schema_ctx(schema_ctx, _message_or_mod) when not is_nil(schema_ctx), do: schema_ctx

  defp extract_schema_ctx(nil, mod) when is_atom(mod) do
    if function_exported?(mod, :__schema__, 0), do: mod.__schema__(), else: nil
  end

  defp extract_schema_ctx(nil, %{__struct__: mod}) when is_atom(mod) do
    if function_exported?(mod, :__schema__, 0), do: mod.__schema__(), else: nil
  end

  defp extract_schema_ctx(nil, _other), do: nil

  defp resolve_message_descriptor(%Message{} = msg, _ctx), do: msg

  defp resolve_message_descriptor(name, %Schema{} = schema) when is_binary(name) do
    Schema.message(schema, name) ||
      throw({:error, "Message #{inspect(name)} not found in schema"})
  end

  defp resolve_message_descriptor(module, _ctx) when is_atom(module) do
    if function_exported?(module, :__fields__, 0) do
      fields = module.__fields__()

      name =
        if function_exported?(module, :__message_name__, 0),
          do: module.__message_name__(),
          else: inspect(module)

      fields_by_num =
        Enum.flat_map(fields, fn
          %Field{kind: :oneof, fields: oneof_fields} -> Enum.map(oneof_fields, &{&1.number, &1})
          %Field{number: num} = f when is_integer(num) -> [{num, f}]
          _ -> []
        end)
        |> Map.new()

      nested_enums =
        if function_exported?(module, :__nested_enums__, 0) do
          module.__nested_enums__()
        else
          []
        end

      %Message{
        name: name,
        fields: fields,
        fields_by_number: fields_by_num,
        nested_enums: nested_enums
      }
    else
      throw({:error, "Module #{inspect(module)} does not implement protobuf __fields__/0"})
    end
  end

  defp resolve_submessage_descriptor({:message, type_name}, schema_ctx) do
    resolve_submessage_descriptor(type_name, schema_ctx)
  end

  defp resolve_submessage_descriptor(type_name, %Schema{} = schema) when is_binary(type_name) do
    Schema.message(schema, type_name) ||
      throw({:error, "Message #{inspect(type_name)} not found in schema"})
  end

  defp resolve_submessage_descriptor(type_name, _ctx) when is_binary(type_name) do
    throw(
      {:error,
       "Cannot resolve nested message #{inspect(type_name)} without a %SimpleGrpc.Schema{} context"}
    )
  end

  @doc false
  def kw_to_map(kw) when is_list(kw) do
    if Keyword.keyword?(kw) do
      Enum.reduce(kw, %{}, fn {k, v}, acc ->
        atom_key = if is_binary(k), do: String.to_atom(k), else: k
        Map.put(acc, atom_key, v)
      end)
    else
      %{}
    end
  end
end
