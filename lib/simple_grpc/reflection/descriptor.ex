defmodule SimpleGrpc.Reflection.Descriptor do
  @moduledoc """
  Generates binary `google.protobuf.FileDescriptorProto` descriptors dynamically
  from in-memory `%SimpleGrpc.Schema{}` metadata without needing `.proto` files on disk.
  """

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.{Service, Message, RPC, Field}
  alias SimpleGrpc.Schema.Enum, as: SchemaEnum
  alias SimpleGrpc.Protobuf

  # ===========================================================================
  # Descriptor Proto Message Definitions (for Protobuf.encode!)
  # ===========================================================================

  @field_descriptor_proto %Message{
    name: "FieldDescriptorProto",
    fields: [
      %Field{name: :name, number: 1, type: "string", kind: :field},
      %Field{name: :number, number: 3, type: "int32", kind: :field},
      %Field{name: :label, number: 4, type: "int32", kind: :field},
      %Field{name: :type, number: 5, type: "int32", kind: :field},
      %Field{name: :type_name, number: 6, type: "string", kind: :field},
      %Field{name: :extendee, number: 2, type: "string", kind: :field},
      %Field{name: :default_value, number: 7, type: "string", kind: :field},
      %Field{name: :oneof_index, number: 9, type: "int32", prefix: :optional, kind: :field},
      %Field{name: :json_name, number: 10, type: "string", kind: :field},
      %Field{name: :proto3_optional, number: 17, type: "bool", prefix: :optional, kind: :field}
    ]
  }

  @oneof_descriptor_proto %Message{
    name: "OneofDescriptorProto",
    fields: [
      %Field{name: :name, number: 1, type: "string", kind: :field}
    ]
  }

  @enum_value_descriptor_proto %Message{
    name: "EnumValueDescriptorProto",
    fields: [
      %Field{name: :name, number: 1, type: "string", kind: :field},
      %Field{name: :number, number: 2, type: "int32", kind: :field}
    ]
  }

  @enum_descriptor_proto %Message{
    name: "EnumDescriptorProto",
    fields: [
      %Field{name: :name, number: 1, type: "string", kind: :field},
      %Field{
        name: :value,
        number: 2,
        type: "EnumValueDescriptorProto",
        prefix: :repeated,
        kind: :field
      }
    ]
  }

  @method_descriptor_proto %Message{
    name: "MethodDescriptorProto",
    fields: [
      %Field{name: :name, number: 1, type: "string", kind: :field},
      %Field{name: :input_type, number: 2, type: "string", kind: :field},
      %Field{name: :output_type, number: 3, type: "string", kind: :field},
      %Field{name: :client_streaming, number: 5, type: "bool", kind: :field},
      %Field{name: :server_streaming, number: 6, type: "bool", kind: :field}
    ]
  }

  @service_descriptor_proto %Message{
    name: "ServiceDescriptorProto",
    fields: [
      %Field{name: :name, number: 1, type: "string", kind: :field},
      %Field{
        name: :method,
        number: 2,
        type: "MethodDescriptorProto",
        prefix: :repeated,
        kind: :field
      }
    ]
  }

  @descriptor_proto %Message{
    name: "DescriptorProto",
    fields: [
      %Field{name: :name, number: 1, type: "string", kind: :field},
      %Field{
        name: :field,
        number: 2,
        type: "FieldDescriptorProto",
        prefix: :repeated,
        kind: :field
      },
      %Field{
        name: :nested_type,
        number: 3,
        type: "DescriptorProto",
        prefix: :repeated,
        kind: :field
      },
      %Field{
        name: :enum_type,
        number: 4,
        type: "EnumDescriptorProto",
        prefix: :repeated,
        kind: :field
      },
      %Field{
        name: :oneof_decl,
        number: 8,
        type: "OneofDescriptorProto",
        prefix: :repeated,
        kind: :field
      },
      %Field{name: :options, number: 7, type: "MessageOptions", kind: :field}
    ]
  }

  @file_descriptor_proto %Message{
    name: "FileDescriptorProto",
    fields: [
      %Field{name: :name, number: 1, type: "string", kind: :field},
      %Field{name: :package, number: 2, type: "string", kind: :field},
      %Field{name: :dependency, number: 3, type: "string", prefix: :repeated, kind: :field},
      %Field{
        name: :message_type,
        number: 4,
        type: "DescriptorProto",
        prefix: :repeated,
        kind: :field
      },
      %Field{
        name: :enum_type,
        number: 5,
        type: "EnumDescriptorProto",
        prefix: :repeated,
        kind: :field
      },
      %Field{
        name: :service,
        number: 6,
        type: "ServiceDescriptorProto",
        prefix: :repeated,
        kind: :field
      },
      %Field{name: :syntax, number: 12, type: "string", kind: :field}
    ]
  }

  @descriptor_schema %Schema{
    syntax: :proto3,
    messages: %{
      "FileDescriptorProto" => @file_descriptor_proto,
      "DescriptorProto" => @descriptor_proto,
      "FieldDescriptorProto" => @field_descriptor_proto,
      "OneofDescriptorProto" => @oneof_descriptor_proto,
      "EnumDescriptorProto" => @enum_descriptor_proto,
      "EnumValueDescriptorProto" => @enum_value_descriptor_proto,
      "ServiceDescriptorProto" => @service_descriptor_proto,
      "MethodDescriptorProto" => @method_descriptor_proto,
      "MessageOptions" => %Message{
        name: "MessageOptions",
        fields: [%Field{name: :map_entry, number: 7, type: "bool", kind: :field}]
      }
    }
  }

  # ===========================================================================
  # Encoding Functions
  # ===========================================================================

  @doc """
  Converts a `%SimpleGrpc.Schema{}` into binary-encoded `FileDescriptorProto` bytes.
  """
  def encode_file_descriptor(%Schema{} = schema, filename \\ nil) do
    fdp_map = schema_to_file_descriptor_proto(schema, filename)
    Protobuf.encode!(@file_descriptor_proto, fdp_map, @descriptor_schema)
  end

  @doc """
  Converts a `%SimpleGrpc.Schema{}` into a map structure adhering to `FileDescriptorProto`.
  """
  def schema_to_file_descriptor_proto(%Schema{} = schema, filename \\ nil) do
    package = schema.package || ""

    name =
      filename ||
        (if package != "", do: "#{package}.proto", else: "service.proto")

    # Filter out duplicate alias keys from schema.services, schema.messages, schema.enums
    services =
      schema.services
      |> Map.values()
      |> Enum.uniq_by(& &1.name)
      |> Enum.map(&service_to_proto(&1, package))

    top_messages =
      schema.messages
      |> Map.values()
      |> Enum.uniq_by(&(&1.full_name || &1.name))
      # only top-level messages (not nested messages)
      |> Enum.filter(fn msg ->
        not String.contains?(msg.name, ".") and
          (is_nil(msg.full_name) or msg.full_name == msg.name or
             msg.full_name == "#{package}.#{msg.name}")
      end)
      |> Enum.map(&message_to_proto(&1, package))

    top_enums =
      schema.enums
      |> Map.values()
      |> Enum.uniq_by(fn
        %SchemaEnum{name: name} -> name
        map when is_map(map) -> map[:name] || map["name"]
      end)
      |> Enum.map(&enum_to_proto/1)

    dependencies =
      extract_dependencies(schema)

    %{
      name: name,
      package: package,
      dependency: dependencies,
      message_type: top_messages,
      enum_type: top_enums,
      service: services,
      syntax: "proto3"
    }
  end

  # ---------------------------------------------------------------------------
  # Message Conversion
  # ---------------------------------------------------------------------------

  defp message_to_proto(%Message{name: name, fields: fields, nested_messages: nested_msgs, nested_enums: nested_enums}, package) do
    # Separate oneof definitions
    {oneof_decls, regular_and_oneof_fields, synthetic_map_entries} =
      process_message_fields(fields, name, package)

    nested_protos =
      Enum.map(nested_msgs, &message_to_proto(&1, package)) ++ synthetic_map_entries

    enum_protos = Enum.map(nested_enums, &enum_to_proto/1)

    %{
      name: name,
      field: regular_and_oneof_fields,
      nested_type: nested_protos,
      enum_type: enum_protos,
      oneof_decl: oneof_decls,
      options: %{}
    }
  end

  defp process_message_fields(fields, parent_msg_name, package) do
    Enum.reduce(fields, {[], [], []}, fn
      %Field{kind: :oneof, name: oneof_name, fields: subfields}, {oneof_acc, fields_acc, map_acc} ->
        oneof_index = length(oneof_acc)
        oneof_decl = %{name: to_string(oneof_name)}

        oneof_fields =
          Enum.map(subfields, fn subfield ->
            field_to_proto(subfield, package, oneof_index: oneof_index)
          end)

        {oneof_acc ++ [oneof_decl], fields_acc ++ oneof_fields, map_acc}

      %Field{kind: :map, name: field_name, number: num, key_type: k_type, val_type: v_type} = map_field, {oneof_acc, fields_acc, map_acc} ->
        # Protobuf represents maps as synthetic repeated MapEntry nested messages
        entry_name = Macro.camelize(to_string(field_name)) <> "Entry"
        full_entry_type = if package != "", do: ".#{package}.#{parent_msg_name}.#{entry_name}", else: ".#{parent_msg_name}.#{entry_name}"

        map_entry_desc = %{
          name: entry_name,
          field: [
            field_to_proto(%Field{name: :key, number: 1, type: k_type, kind: :field}, package, []),
            field_to_proto(%Field{name: :value, number: 2, type: v_type, kind: :field}, package, [])
          ],
          nested_type: [],
          enum_type: [],
          oneof_decl: [],
          options: %{map_entry: true}
        }

        fdp = %{
          name: to_string(map_field.name),
          number: num,
          label: 3, # LABEL_REPEATED
          type: 11, # TYPE_MESSAGE
          type_name: full_entry_type,
          json_name: to_json_name(map_field.name),
          proto3_optional: false
        }

        {oneof_acc, fields_acc ++ [fdp], map_acc ++ [map_entry_desc]}

      %Field{} = field, {oneof_acc, fields_acc, map_acc} ->
        fdp = field_to_proto(field, package, [])
        {oneof_acc, fields_acc ++ [fdp], map_acc}
    end)
  end

  defp field_to_proto(%Field{name: fname, number: num, type: type, prefix: prefix}, package, opts) do
    oneof_index = Keyword.get(opts, :oneof_index)
    type_code = type_name_to_type_code(type)

    label =
      cond do
        prefix == :repeated -> 3 # LABEL_REPEATED
        prefix == :optional -> 1 # LABEL_OPTIONAL
        true -> 1 # LABEL_OPTIONAL (proto3 default)
      end

    type_name =
      if type_code in [11, 14] do
        format_full_type_name(type, package)
      else
        ""
      end

    proto3_optional = prefix == :optional and is_nil(oneof_index)

    res = %{
      name: to_string(fname),
      number: num,
      label: label,
      type: type_code,
      type_name: type_name,
      json_name: to_json_name(fname),
      proto3_optional: proto3_optional
    }

    if not is_nil(oneof_index) do
      Map.put(res, :oneof_index, oneof_index)
    else
      res
    end
  end

  # ---------------------------------------------------------------------------
  # Enum Conversion
  # ---------------------------------------------------------------------------

  defp enum_to_proto(%SchemaEnum{name: name, fields: fields}) do
    values =
      Enum.map(fields, fn {val_name, val_num} ->
        %{name: to_string(val_name), number: val_num}
      end)

    %{name: to_string(name), value: values}
  end

  defp enum_to_proto(map) when is_map(map) do
    name = map[:name] || map["name"] || "Enum"
    fields = map[:fields] || map["fields"] || []

    values =
      Enum.map(fields, fn
        {k, v} -> %{name: to_string(k), number: v}
        %{name: k, number: v} -> %{name: to_string(k), number: v}
      end)

    %{name: to_string(name), value: values}
  end

  # ---------------------------------------------------------------------------
  # Service Conversion
  # ---------------------------------------------------------------------------

  defp service_to_proto(%Service{name: name, rpcs: rpcs}, package) do
    methods =
      Enum.map(rpcs, fn {_r_name, %RPC{} = rpc} ->
        %{
          name: rpc.name,
          input_type: format_full_type_name(rpc.input, package),
          output_type: format_full_type_name(rpc.output, package),
          client_streaming: rpc.request_stream || false,
          server_streaming: rpc.response_stream || false
        }
      end)

    %{name: name, method: methods}
  end

  # ---------------------------------------------------------------------------
  # Type and Name Helpers
  # ---------------------------------------------------------------------------

  defp type_name_to_type_code("double"), do: 1
  defp type_name_to_type_code("float"), do: 2
  defp type_name_to_type_code("int64"), do: 3
  defp type_name_to_type_code("uint64"), do: 4
  defp type_name_to_type_code("int32"), do: 5
  defp type_name_to_type_code("fixed64"), do: 6
  defp type_name_to_type_code("fixed32"), do: 7
  defp type_name_to_type_code("bool"), do: 8
  defp type_name_to_type_code("string"), do: 9
  defp type_name_to_type_code("bytes"), do: 12
  defp type_name_to_type_code("uint32"), do: 13
  defp type_name_to_type_code("sfixed32"), do: 15
  defp type_name_to_type_code("sfixed64"), do: 16
  defp type_name_to_type_code("sint32"), do: 17
  defp type_name_to_type_code("sint64"), do: 18

  defp type_name_to_type_code(type) when is_binary(type) do
    if is_known_enum_type?(type) do
      14 # TYPE_ENUM
    else
      11 # TYPE_MESSAGE
    end
  end

  defp type_name_to_type_code(_other), do: 11

  defp is_known_enum_type?(type) do
    # Heuristic for enum naming convention or enum prefixes
    String.contains?(type, "Priority") or String.contains?(type, "Status") or
      String.contains?(type, "Enum") or String.contains?(type, "NullValue")
  end

  defp format_full_type_name(type, _package) when is_binary(type) and binary_part(type, 0, 1) == "." do
    type
  end

  defp format_full_type_name(type, package) when is_binary(type) do
    cond do
      String.starts_with?(type, "google.protobuf.") ->
        ".#{type}"

      String.contains?(type, ".") ->
        ".#{type}"

      package != "" ->
        ".#{package}.#{type}"

      true ->
        ".#{type}"
    end
  end

  defp format_full_type_name(other, _package), do: ".#{other}"

  defp to_json_name(field_name) do
    field_name
    |> to_string()
    |> Macro.camelize()
    |> lowercase_first()
  end

  defp lowercase_first(""), do: ""
  defp lowercase_first(<<first::utf8, rest::binary>>) do
    String.downcase(<<first::utf8>>) <> rest
  end

  defp extract_dependencies(%Schema{imports: imports}) when is_list(imports) do
    imports
  end

  defp extract_dependencies(_), do: []
end
