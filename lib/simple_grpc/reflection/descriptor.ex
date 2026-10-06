defmodule SimpleGrpc.Reflection.Descriptor.Builder do
  @moduledoc false
  alias SimpleGrpc.Schema.{Message, Field}

  def make_message(name, fields) do
    by_num =
      fields
      |> Enum.flat_map(fn
        %Field{kind: :oneof, fields: oneof_fields} -> Enum.map(oneof_fields, &{&1.number, &1})
        %Field{number: num} = f when is_integer(num) -> [{num, f}]
        _ -> []
      end)
      |> Map.new()

    by_name =
      fields
      |> Enum.flat_map(fn
        %Field{kind: :oneof, fields: oneof_fields} -> Enum.map(oneof_fields, &{&1.name, &1})
        %Field{name: fname} = f when is_atom(fname) -> [{fname, f}]
        _ -> []
      end)
      |> Map.new()

    %Message{
      name: name,
      full_name: name,
      fields: fields,
      fields_by_number: by_num,
      fields_by_name: by_name
    }
  end
end

defmodule SimpleGrpc.Reflection.Descriptor do
  @moduledoc """
  Generates binary `google.protobuf.FileDescriptorProto` descriptors dynamically
  from in-memory `%SimpleGrpc.Schema{}` metadata without needing `.proto` files on disk.
  """

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.{Service, Message, RPC, Field}
  alias SimpleGrpc.Schema.Enum, as: SchemaEnum
  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Reflection.Descriptor.Builder

  # ===========================================================================
  # Descriptor Proto Message Definitions (for Protobuf.encode!)
  # ===========================================================================

  @field_descriptor_proto Builder.make_message("FieldDescriptorProto", [
                            %Field{name: :name, number: 1, type: "string", kind: :field},
                            %Field{name: :number, number: 3, type: "int32", kind: :field},
                            %Field{name: :label, number: 4, type: "int32", kind: :field},
                            %Field{name: :type, number: 5, type: "int32", kind: :field},
                            %Field{name: :type_name, number: 6, type: "string", kind: :field},
                            %Field{name: :extendee, number: 2, type: "string", kind: :field},
                            %Field{name: :default_value, number: 7, type: "string", kind: :field},
                            %Field{
                              name: :oneof_index,
                              number: 9,
                              type: "int32",
                              prefix: :optional,
                              kind: :field
                            },
                            %Field{name: :json_name, number: 10, type: "string", kind: :field},
                            %Field{
                              name: :proto3_optional,
                              number: 17,
                              type: "bool",
                              prefix: :optional,
                              kind: :field
                            }
                          ])

  @oneof_descriptor_proto Builder.make_message("OneofDescriptorProto", [
                            %Field{name: :name, number: 1, type: "string", kind: :field}
                          ])

  @enum_value_descriptor_proto Builder.make_message("EnumValueDescriptorProto", [
                                 %Field{name: :name, number: 1, type: "string", kind: :field},
                                 %Field{name: :number, number: 2, type: "int32", kind: :field}
                               ])

  @enum_descriptor_proto Builder.make_message("EnumDescriptorProto", [
                           %Field{name: :name, number: 1, type: "string", kind: :field},
                           %Field{
                             name: :value,
                             number: 2,
                             type: "EnumValueDescriptorProto",
                             prefix: :repeated,
                             kind: :field
                           }
                         ])

  @method_descriptor_proto Builder.make_message("MethodDescriptorProto", [
                             %Field{name: :name, number: 1, type: "string", kind: :field},
                             %Field{name: :input_type, number: 2, type: "string", kind: :field},
                             %Field{name: :output_type, number: 3, type: "string", kind: :field},
                             %Field{
                               name: :client_streaming,
                               number: 5,
                               type: "bool",
                               kind: :field
                             },
                             %Field{
                               name: :server_streaming,
                               number: 6,
                               type: "bool",
                               kind: :field
                             }
                           ])

  @service_descriptor_proto Builder.make_message("ServiceDescriptorProto", [
                              %Field{name: :name, number: 1, type: "string", kind: :field},
                              %Field{
                                name: :method,
                                number: 2,
                                type: "MethodDescriptorProto",
                                prefix: :repeated,
                                kind: :field
                              }
                            ])

  @descriptor_proto Builder.make_message("DescriptorProto", [
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
                    ])

  @file_descriptor_proto Builder.make_message("FileDescriptorProto", [
                           %Field{name: :name, number: 1, type: "string", kind: :field},
                           %Field{name: :package, number: 2, type: "string", kind: :field},
                           %Field{
                             name: :dependency,
                             number: 3,
                             type: "string",
                             prefix: :repeated,
                             kind: :field
                           },
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
                         ])

  @message_options Builder.make_message("MessageOptions", [
                     %Field{name: :map_entry, number: 7, type: "bool", kind: :field}
                   ])

  @descriptor_schema Schema.finalize_schema(%Schema{
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
                         "MessageOptions" => @message_options
                       }
                     })

  @doc """
  Returns the internal descriptor schema used for encoding and decoding FileDescriptorProtos.
  """
  def descriptor_schema, do: @descriptor_schema

  @doc """
  Returns the FileDescriptorProto message definition.
  """
  def file_descriptor_proto_message, do: @file_descriptor_proto

  # ===========================================================================
  # Encoding Functions
  # ===========================================================================

  @doc """
  Converts a `%SimpleGrpc.Schema{}` into binary-encoded `FileDescriptorProto` bytes.
  """
  def encode_file_descriptor(%Schema{} = schema, filename \\ nil) do
    fdp_map = schema_to_file_descriptor_proto(schema, filename)

    Protobuf.encode!(
      @descriptor_schema.messages["FileDescriptorProto"],
      fdp_map,
      @descriptor_schema
    )
  end

  @doc """
  Decodes a binary `FileDescriptorProto` payload into a `%SimpleGrpc.Schema{}` struct.
  """
  def decode_file_descriptor(binary) when is_binary(binary) do
    decoded_map =
      Protobuf.decode!(
        @descriptor_schema.messages["FileDescriptorProto"],
        binary,
        @descriptor_schema
      )

    file_descriptor_proto_to_schema(decoded_map)
  end

  @doc """
  Converts a decoded `FileDescriptorProto` map structure back into a `%SimpleGrpc.Schema{}` struct.
  """
  def file_descriptor_proto_to_schema(fdp_map) when is_map(fdp_map) do
    package =
      case Map.get(fdp_map, :package) || Map.get(fdp_map, "package") do
        "" -> nil
        nil -> nil
        pkg -> pkg
      end

    syntax =
      case Map.get(fdp_map, :syntax) || Map.get(fdp_map, "syntax") do
        "proto2" -> :proto2
        :proto2 -> :proto2
        _ -> :proto3
      end

    imports = Map.get(fdp_map, :dependency) || Map.get(fdp_map, "dependency") || []
    filename = Map.get(fdp_map, :name) || Map.get(fdp_map, "name")
    files = if filename && filename != "", do: [filename], else: []

    # 1. Enums
    raw_enums = Map.get(fdp_map, :enum_type) || Map.get(fdp_map, "enum_type") || []
    top_enums = Enum.map(raw_enums, &proto_to_enum(&1, package))

    top_enums_map =
      Enum.flat_map(top_enums, fn enum ->
        full_name = if package, do: "#{package}.#{enum.name}", else: enum.name

        if full_name != enum.name do
          [{enum.name, enum}, {full_name, enum}]
        else
          [{enum.name, enum}]
        end
      end)

    # 2. Messages
    raw_messages = Map.get(fdp_map, :message_type) || Map.get(fdp_map, "message_type") || []
    top_messages = Enum.map(raw_messages, &proto_to_message(&1, package, nil))

    messages_map =
      Enum.flat_map(top_messages, &flatten_message_entries/1)
      |> Map.new()

    nested_enums_from_messages =
      Enum.flat_map(top_messages, &flatten_nested_enums/1)

    enums_map = Map.new(top_enums_map ++ nested_enums_from_messages)

    # 3. Services
    raw_services = Map.get(fdp_map, :service) || Map.get(fdp_map, "service") || []

    services_map =
      raw_services
      |> Enum.map(&proto_to_service(&1, package))
      |> Enum.flat_map(fn service ->
        if service.full_name && service.full_name != service.name do
          [{service.name, service}, {service.full_name, service}]
        else
          [{service.name, service}]
        end
      end)
      |> Map.new()

    %Schema{
      package: package,
      syntax: syntax,
      services: services_map,
      messages: messages_map,
      enums: enums_map,
      options: %{},
      imports: imports,
      files: files
    }
  end

  @doc """
  Converts a `%SimpleGrpc.Schema{}` into a map structure adhering to `FileDescriptorProto`.
  """
  def schema_to_file_descriptor_proto(%Schema{} = schema, filename \\ nil) do
    package = schema.package || ""

    name =
      filename ||
        if package != "", do: "#{package}.proto", else: "service.proto"

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

  defp message_to_proto(
         %Message{
           name: name,
           fields: fields,
           nested_messages: nested_msgs,
           nested_enums: nested_enums
         },
         package
       ) do
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
      %Field{kind: :oneof, name: oneof_name, fields: subfields},
      {oneof_acc, fields_acc, map_acc} ->
        oneof_index = length(oneof_acc)
        oneof_decl = %{name: to_string(oneof_name)}

        oneof_fields =
          Enum.map(subfields, fn subfield ->
            field_to_proto(subfield, package, oneof_index: oneof_index)
          end)

        {oneof_acc ++ [oneof_decl], fields_acc ++ oneof_fields, map_acc}

      %Field{kind: :map, name: field_name, number: num, key_type: k_type, val_type: v_type} =
          map_field,
      {oneof_acc, fields_acc, map_acc} ->
        # Protobuf represents maps as synthetic repeated MapEntry nested messages
        entry_name = Macro.camelize(to_string(field_name)) <> "Entry"

        full_entry_type =
          if package != "",
            do: ".#{package}.#{parent_msg_name}.#{entry_name}",
            else: ".#{parent_msg_name}.#{entry_name}"

        map_entry_desc = %{
          name: entry_name,
          field: [
            field_to_proto(
              %Field{name: :key, number: 1, type: k_type, kind: :field},
              package,
              []
            ),
            field_to_proto(
              %Field{name: :value, number: 2, type: v_type, kind: :field},
              package,
              []
            )
          ],
          nested_type: [],
          enum_type: [],
          oneof_decl: [],
          options: %{map_entry: true}
        }

        fdp = %{
          name: to_string(map_field.name),
          number: num,
          # LABEL_REPEATED
          label: 3,
          # TYPE_MESSAGE
          type: 11,
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
        # LABEL_REPEATED
        prefix == :repeated -> 3
        # LABEL_OPTIONAL
        prefix == :optional -> 1
        # LABEL_OPTIONAL (proto3 default)
        true -> 1
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
  # Protobuf Map to Schema Converters
  # ---------------------------------------------------------------------------

  defp proto_to_enum(enum_proto, _package) do
    name = Map.get(enum_proto, :name) || Map.get(enum_proto, "name")
    raw_values = Map.get(enum_proto, :value) || Map.get(enum_proto, "value") || []

    fields =
      Enum.map(raw_values, fn val ->
        v_name = Map.get(val, :name) || Map.get(val, "name")
        v_num = Map.get(val, :number) || Map.get(val, "number")
        {String.to_atom(to_string(v_name)), v_num}
      end)

    %SchemaEnum{
      name: to_string(name),
      fields: fields
    }
  end

  defp proto_to_service(svc_proto, package) do
    name = Map.get(svc_proto, :name) || Map.get(svc_proto, "name")
    full_name = if package && package != "", do: "#{package}.#{name}", else: to_string(name)
    raw_methods = Map.get(svc_proto, :method) || Map.get(svc_proto, "method") || []

    rpcs =
      Enum.map(raw_methods, fn method ->
        m_name = Map.get(method, :name) || Map.get(method, "name")
        in_type = clean_type_name(Map.get(method, :input_type) || Map.get(method, "input_type"))

        out_type =
          clean_type_name(Map.get(method, :output_type) || Map.get(method, "output_type"))

        c_stream =
          Map.get(method, :client_streaming) || Map.get(method, "client_streaming") || false

        s_stream =
          Map.get(method, :server_streaming) || Map.get(method, "server_streaming") || false

        path = "/#{full_name}/#{m_name}"

        rpc = %RPC{
          name: to_string(m_name),
          input: in_type,
          output: out_type,
          request_stream: c_stream,
          response_stream: s_stream,
          path: path
        }

        {to_string(m_name), rpc}
      end)
      |> Map.new()

    %Service{
      name: to_string(name),
      full_name: full_name,
      package: package,
      rpcs: rpcs
    }
  end

  defp proto_to_message(msg_proto, package, parent_prefix) do
    name = to_string(Map.get(msg_proto, :name) || Map.get(msg_proto, "name"))

    full_name =
      cond do
        parent_prefix -> "#{parent_prefix}.#{name}"
        package && package != "" -> "#{package}.#{name}"
        true -> name
      end

    raw_nested_types = Map.get(msg_proto, :nested_type) || Map.get(msg_proto, "nested_type") || []
    raw_enum_types = Map.get(msg_proto, :enum_type) || Map.get(msg_proto, "enum_type") || []
    raw_fields = Map.get(msg_proto, :field) || Map.get(msg_proto, "field") || []
    raw_oneofs = Map.get(msg_proto, :oneof_decl) || Map.get(msg_proto, "oneof_decl") || []

    # Detect synthetic map entries
    map_entry_descriptors =
      Enum.filter(raw_nested_types, fn n ->
        opts = Map.get(n, :options) || Map.get(n, "options") || %{}
        Map.get(opts, :map_entry) == true or Map.get(opts, "map_entry") == true
      end)
      |> Map.new(fn n ->
        n_name = to_string(Map.get(n, :name) || Map.get(n, "name"))
        fields = Map.get(n, :field) || Map.get(n, "field") || []

        k_field =
          Enum.find(fields, fn f -> (Map.get(f, :number) || Map.get(f, "number")) == 1 end)

        v_field =
          Enum.find(fields, fn f -> (Map.get(f, :number) || Map.get(f, "number")) == 2 end)

        k_type =
          if k_field,
            do:
              type_code_to_type_name(
                Map.get(k_field, :type) || Map.get(k_field, "type"),
                Map.get(k_field, :type_name) || Map.get(k_field, "type_name")
              ),
            else: "string"

        v_type =
          if v_field,
            do:
              type_code_to_type_name(
                Map.get(v_field, :type) || Map.get(v_field, "type"),
                Map.get(v_field, :type_name) || Map.get(v_field, "type_name")
              ),
            else: "string"

        {n_name, {k_type, v_type}}
      end)

    # Real nested messages (excluding map entries)
    nested_messages =
      raw_nested_types
      |> Enum.reject(fn n ->
        n_name = to_string(Map.get(n, :name) || Map.get(n, "name"))
        Map.has_key?(map_entry_descriptors, n_name)
      end)
      |> Enum.map(&proto_to_message(&1, package, full_name))

    nested_enums =
      Enum.map(raw_enum_types, &proto_to_enum(&1, package))

    # Parse message fields
    fields = parse_proto_fields(raw_fields, raw_oneofs, map_entry_descriptors)

    fields_by_num =
      fields
      |> Enum.flat_map(fn
        %Field{kind: :oneof, fields: oneof_fields} -> Enum.map(oneof_fields, &{&1.number, &1})
        %Field{number: num} = f when is_integer(num) -> [{num, f}]
        _ -> []
      end)
      |> Map.new()

    fields_by_name =
      fields
      |> Enum.flat_map(fn
        %Field{kind: :oneof, fields: oneof_fields} -> Enum.map(oneof_fields, &{&1.name, &1})
        %Field{name: fname} = f when is_atom(fname) -> [{fname, f}]
        _ -> []
      end)
      |> Map.new()

    %Message{
      name: name,
      full_name: full_name,
      fields: fields,
      fields_by_number: fields_by_num,
      fields_by_name: fields_by_name,
      nested_messages: nested_messages,
      nested_enums: nested_enums
    }
  end

  defp parse_proto_fields(raw_fields, raw_oneofs, map_entry_descriptors) do
    # Group oneof fields vs regular / map fields
    {oneof_field_groups, regular_or_map_fields} =
      Enum.reduce(raw_fields, {%{}, []}, fn f, {oneof_acc, reg_acc} ->
        f_name = Map.get(f, :name) || Map.get(f, "name")
        f_num = Map.get(f, :number) || Map.get(f, "number")
        f_label = Map.get(f, :label) || Map.get(f, "label")
        f_type = Map.get(f, :type) || Map.get(f, "type")
        f_type_name = Map.get(f, :type_name) || Map.get(f, "type_name")
        f_proto3_opt = Map.get(f, :proto3_optional) || Map.get(f, "proto3_optional") || false
        f_oneof_idx = Map.get(f, :oneof_index) || Map.get(f, "oneof_index")

        type_str = type_code_to_type_name(f_type, f_type_name)

        # Check if map field
        type_base_name =
          if is_binary(f_type_name),
            do: f_type_name |> String.split(".") |> List.last(),
            else: nil

        cond do
          type_base_name && Map.has_key?(map_entry_descriptors, type_base_name) ->
            {k_type, v_type} = Map.get(map_entry_descriptors, type_base_name)

            map_field = %Field{
              name: String.to_atom(to_string(f_name)),
              number: f_num,
              kind: :map,
              key_type: k_type,
              val_type: v_type
            }

            {oneof_acc, reg_acc ++ [map_field]}

          not is_nil(f_oneof_idx) and not f_proto3_opt ->
            subfield = %Field{
              name: String.to_atom(to_string(f_name)),
              number: f_num,
              type: type_str,
              prefix: :optional,
              kind: :field
            }

            updated_group = Map.get(oneof_acc, f_oneof_idx, []) ++ [subfield]
            {Map.put(oneof_acc, f_oneof_idx, updated_group), reg_acc}

          true ->
            prefix =
              cond do
                f_label == 3 -> :repeated
                f_proto3_opt -> :optional
                true -> nil
              end

            field = %Field{
              name: String.to_atom(to_string(f_name)),
              number: f_num,
              type: type_str,
              prefix: prefix,
              kind: :field
            }

            {oneof_acc, reg_acc ++ [field]}
        end
      end)

    # Convert oneof groups to %Field{kind: :oneof}
    oneof_fields =
      Enum.map(oneof_field_groups, fn {idx, subfields} ->
        decl = Enum.at(raw_oneofs, idx)
        decl_name = (decl && (Map.get(decl, :name) || Map.get(decl, "name"))) || "oneof_#{idx}"

        %Field{
          name: String.to_atom(to_string(decl_name)),
          kind: :oneof,
          fields: subfields
        }
      end)

    regular_or_map_fields ++ oneof_fields
  end

  defp flatten_message_entries(%Message{} = msg) do
    base_entries =
      if msg.full_name && msg.full_name != msg.name do
        [{msg.name, msg}, {msg.full_name, msg}]
      else
        [{msg.name, msg}]
      end

    nested_entries =
      Enum.flat_map(msg.nested_messages, fn nested ->
        nested_full_name =
          if msg.full_name,
            do: "#{msg.full_name}.#{nested.name}",
            else: "#{msg.name}.#{nested.name}"

        nested = %{nested | full_name: nested_full_name}

        [
          {nested.name, nested},
          {"#{msg.name}.#{nested.name}", nested},
          {nested_full_name, nested}
          | flatten_message_entries(nested)
        ]
      end)

    base_entries ++ nested_entries
  end

  defp flatten_nested_enums(%Message{
         nested_enums: nested_enums,
         nested_messages: nested_messages,
         name: msg_name,
         full_name: full_name
       }) do
    direct =
      Enum.flat_map(nested_enums, fn enum ->
        enum_full =
          if full_name, do: "#{full_name}.#{enum.name}", else: "#{msg_name}.#{enum.name}"

        [
          {enum.name, enum},
          {"#{msg_name}.#{enum.name}", enum},
          {enum_full, enum}
        ]
      end)

    nested = Enum.flat_map(nested_messages, &flatten_nested_enums/1)
    direct ++ nested
  end

  # ---------------------------------------------------------------------------
  # Type and Name Helpers
  # ---------------------------------------------------------------------------

  def type_code_to_type_name(1, _), do: "double"
  def type_code_to_type_name(2, _), do: "float"
  def type_code_to_type_name(3, _), do: "int64"
  def type_code_to_type_name(4, _), do: "uint64"
  def type_code_to_type_name(5, _), do: "int32"
  def type_code_to_type_name(6, _), do: "fixed64"
  def type_code_to_type_name(7, _), do: "fixed32"
  def type_code_to_type_name(8, _), do: "bool"
  def type_code_to_type_name(9, _), do: "string"
  def type_code_to_type_name(11, type_name), do: clean_type_name(type_name)
  def type_code_to_type_name(12, _), do: "bytes"
  def type_code_to_type_name(13, _), do: "uint32"
  def type_code_to_type_name(14, type_name), do: clean_type_name(type_name)
  def type_code_to_type_name(15, _), do: "sfixed32"
  def type_code_to_type_name(16, _), do: "sfixed64"
  def type_code_to_type_name(17, _), do: "sint32"
  def type_code_to_type_name(18, _), do: "sint64"
  def type_code_to_type_name(_other, type_name), do: clean_type_name(type_name)

  def clean_type_name(nil), do: ""

  def clean_type_name(type_name) when is_binary(type_name) do
    String.trim_leading(type_name, ".")
  end

  def clean_type_name(other), do: to_string(other)

  def type_name_to_type_code("double"), do: 1
  def type_name_to_type_code("float"), do: 2
  def type_name_to_type_code("int64"), do: 3
  def type_name_to_type_code("uint64"), do: 4
  def type_name_to_type_code("int32"), do: 5
  def type_name_to_type_code("fixed64"), do: 6
  def type_name_to_type_code("fixed32"), do: 7
  def type_name_to_type_code("bool"), do: 8
  def type_name_to_type_code("string"), do: 9
  def type_name_to_type_code("bytes"), do: 12
  def type_name_to_type_code("uint32"), do: 13
  def type_name_to_type_code("sfixed32"), do: 15
  def type_name_to_type_code("sfixed64"), do: 16
  def type_name_to_type_code("sint32"), do: 17
  def type_name_to_type_code("sint64"), do: 18

  def type_name_to_type_code(type) when is_binary(type) do
    if is_known_enum_type?(type) do
      # TYPE_ENUM
      14
    else
      # TYPE_MESSAGE
      11
    end
  end

  def type_name_to_type_code(_other), do: 11

  defp is_known_enum_type?(type) do
    # Heuristic for enum naming convention or enum prefixes
    String.contains?(type, "Priority") or String.contains?(type, "Status") or
      String.contains?(type, "Enum") or String.contains?(type, "NullValue")
  end

  defp format_full_type_name(type, _package)
       when is_binary(type) and binary_part(type, 0, 1) == "." do
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
