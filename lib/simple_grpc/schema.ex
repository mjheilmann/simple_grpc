defmodule SimpleGrpc.Schema do
  @moduledoc """
  Represents a complete protobuf schema containing services, messages, and enums as runtime metadata.
  """
  import Bitwise

  alias SimpleGrpc.Schema.{Service, Message, Field}
  alias SimpleGrpc.Schema.Enum, as: SchemaEnum

  defstruct [
    :package,
    :compiled_module,
    :edition,
    syntax: :proto3,
    features: %{},
    services: %{},
    messages: %{},
    enums: %{},
    options: %{},
    imports: [],
    files: []
  ]

  @type t :: %__MODULE__{
          package: String.t() | nil,
          syntax: atom(),
          edition: String.t() | nil,
          features: map(),
          services: %{String.t() => Service.t()},
          messages: %{String.t() => Message.t()},
          enums: %{String.t() => SchemaEnum.t()},
          options: map(),
          imports: list(String.t()),
          files: list(String.t())
        }

  @doc """
  Builds a Schema struct from a `.proto` file path, automatically following imports by default.

  Options:
  - `:follow_imports` - (boolean, default: `true`) whether to recursively load imported `.proto` files.
  - `:import_paths` - (list of directories) additional search paths for imported `.proto` files.
  """
  def from_file(file_path, opts \\ []) do
    follow_imports = Keyword.get(opts, :follow_imports, true)
    import_paths = Keyword.get(opts, :import_paths, [])
    visited = MapSet.new([Path.expand(file_path)])

    load_file_with_imports(file_path, follow_imports, import_paths, visited, opts)
  end

  @doc """
  Loads and parses multiple `.proto` files, merging them into a unified `%SimpleGrpc.Schema{}`.

  Options:
  - `:service` - (optional string/atom) when provided, prunes the schema to only the specified service
    and the transitively reachable messages and enums needed by its RPC inputs and outputs.
  - `:follow_imports` - (boolean, default: `true`) whether to recursively load imported `.proto` files.
  - `:import_paths` - (list of directories) additional search paths for imported `.proto` files.
  """
  def from_files(file_paths, opts \\ []) when is_list(file_paths) do
    follow_imports = Keyword.get(opts, :follow_imports, true)
    import_paths = Keyword.get(opts, :import_paths, [])
    target_service = Keyword.get(opts, :service)

    result =
      Enum.reduce_while(file_paths, {:ok, %__MODULE__{}}, fn path, {:ok, acc_schema} ->
        visited = MapSet.new(Enum.map(acc_schema.files ++ [path], &Path.expand/1))

        case load_file_with_imports(path, follow_imports, import_paths, visited, opts) do
          {:ok, schema} ->
            {:cont, {:ok, merge(acc_schema, schema)}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)

    case result do
      {:ok, full_schema} when not is_nil(target_service) ->
        filter_for_service(full_schema, target_service)

      other ->
        other
    end
  end

  @doc """
  Recursively loads all `.proto` files in `dir_path`, merging them into a single `%SimpleGrpc.Schema{}`.

  Options:
  - `:service` - (optional string/atom) when provided, only retains the specified service and the
    transitively reachable messages and enums required by its RPCs.
  - `:follow_imports` - (boolean, default: `true`) whether to recursively load imported `.proto` files.
  - `:import_paths` - (list of directories) additional search paths for imported `.proto` files.
  """
  def from_directory(dir_path, opts \\ []) when is_binary(dir_path) do
    if File.dir?(dir_path) do
      proto_files =
        Path.wildcard(Path.join(dir_path, "**/*.proto"))
        |> Enum.sort()

      opts = Keyword.update(opts, :import_paths, [dir_path], &[dir_path | &1])
      from_files(proto_files, opts)
    else
      {:error, "Directory not found: #{inspect(dir_path)}"}
    end
  end

  @doc """
  Prunes a schema to only retain the specified `service_name` and the transitively
  reachable messages and enums referenced by its RPC inputs and outputs.
  """
  def filter_for_service(%__MODULE__{} = schema, service_name) do
    service = service(schema, service_name)

    if is_nil(service) do
      {:error, "Service #{inspect(service_name)} not found in schema"}
    else
      {needed_messages, needed_enums} = collect_service_dependencies(schema, service)

      filtered_services =
        if service.full_name && service.full_name != service.name do
          %{service.name => service, service.full_name => service}
        else
          %{service.name => service}
        end

      filtered_messages =
        schema.messages
        |> Enum.filter(fn {_key, msg} -> msg in needed_messages end)
        |> Map.new()

      filtered_enums =
        schema.enums
        |> Enum.filter(fn {_key, enum} -> enum in needed_enums end)
        |> Map.new()

      pruned_schema = %{
        schema
        | package: service.package || schema.package,
          services: filtered_services,
          messages: filtered_messages,
          enums: filtered_enums
      }

      {:ok, pruned_schema}
    end
  end

  @doc """
  Merges a list of `SimpleGrpc.Schema` structs into a single unified schema.
  """
  def merge(schemas) when is_list(schemas) do
    Enum.reduce(schemas, %__MODULE__{}, fn schema, acc ->
      merge(acc, schema)
    end)
  end

  @doc """
  Merges two `SimpleGrpc.Schema` structs into a single unified schema.
  """
  def merge(%__MODULE__{} = s1, %__MODULE__{} = s2) do
    %__MODULE__{
      package: s1.package || s2.package,
      syntax: s1.syntax || s2.syntax || :proto3,
      edition: s1.edition || s2.edition,
      features: Map.merge(s2.features || %{}, s1.features || %{}),
      services: Map.merge(s1.services, s2.services),
      messages: Map.merge(s1.messages, s2.messages),
      enums: Map.merge(s1.enums, s2.enums),
      options: Map.merge(s1.options, s2.options),
      imports: Enum.uniq(s1.imports ++ s2.imports),
      files: Enum.uniq(s1.files ++ s2.files)
    }
    |> finalize_schema()
  end

  @doc """
  Builds a Schema struct from a proto string.
  """
  def from_string(proto_string, opts \\ []) do
    {syntax, parser, edition} =
      case Keyword.get(opts, :syntax) do
        nil ->
          cond do
            Regex.match?(~r/.*syntax\s*=\s*["']proto2["'].*/, proto_string) ->
              {:proto2, SimpleGrpc.Parser.Proto2, nil}

            captures =
                Regex.named_captures(
                  ~r/.*(?:syntax\s*=\s*["']editions["']|edition\s*=\s*["'](?<edition>[^"']+)["']).*/,
                  proto_string
                ) ->
              ed = Map.get(captures, "edition")
              edition = if is_nil(ed) or ed == "", do: "2023", else: ed
              {:editions, SimpleGrpc.Parser.Editions, edition}

            true ->
              {:proto3, SimpleGrpc.Parser.Proto3, nil}
          end

        :proto2 ->
          {:proto2, SimpleGrpc.Parser.Proto2, nil}

        :editions ->
          {:editions, SimpleGrpc.Parser.Editions, Keyword.get(opts, :edition, "2023")}

        {:editions, ed} ->
          {:editions, SimpleGrpc.Parser.Editions, ed}

        s ->
          {s, SimpleGrpc.Parser.Proto3, nil}
      end

    if syntax == :editions and edition not in ["2023", "2024"] do
      {:error,
       "invalid proto file, unsupported protobuf edition: #{inspect(edition)} (supported editions: 2023, 2024)"}
    else
      case parser.parse(proto_string) do
        {:ok, ast, "", %{}, _, _} ->
          {:ok, from_ast(ast, syntax: syntax, edition: edition)}

        {:ok, _ast, rest, _, _, _} ->
          {:error, "incomplete parse, remaining: #{inspect(rest)}"}

        err ->
          {:error, "failed to parse proto string: #{inspect(err)}"}
      end
    end
  end

  @doc """
  Builds a Schema struct from parser AST.
  """
  def from_ast(ast, opts \\ []) when is_list(ast) do
    edition =
      Keyword.get(opts, :edition) ||
        Enum.find_value(ast, fn
          {:edition, ed} when is_binary(ed) -> ed
          _ -> nil
        end)

    syntax =
      cond do
        edition != nil -> :editions
        Keyword.has_key?(opts, :syntax) -> Keyword.get(opts, :syntax)
        true -> :proto3
      end

    package =
      Enum.find_value(ast, fn
        {:package, pkg} when is_binary(pkg) -> pkg
        _ -> nil
      end)

    imports =
      Enum.flat_map(ast, fn
        {:import, kw} when is_list(kw) ->
          case kw[:path] do
            p when is_binary(p) -> [p]
            _ -> []
          end

        _ ->
          []
      end)

    services =
      ast
      |> Enum.filter(fn
        {:service, _} -> true
        _ -> false
      end)
      |> Enum.flat_map(fn s_ast ->
        service = Service.parse(s_ast, package)

        if service.full_name != service.name do
          [{service.name, service}, {service.full_name, service}]
        else
          [{service.name, service}]
        end
      end)
      |> Map.new()

    parsed_top_messages =
      ast
      |> Enum.filter(fn
        {:message, _} -> true
        _ -> false
      end)
      |> Enum.map(fn m_ast ->
        msg = Message.parse(m_ast)
        full_name = if package && package != "", do: "#{package}.#{msg.name}", else: msg.name
        %{msg | full_name: full_name}
      end)

    # Collect extend blocks from top level ast and nested messages
    top_extend_blocks =
      ast
      |> Enum.filter(fn
        {:extend, _} -> true
        _ -> false
      end)

    nested_extend_blocks =
      Enum.flat_map(parsed_top_messages, &collect_message_extend_blocks/1)

    all_extend_blocks = top_extend_blocks ++ nested_extend_blocks

    top_messages =
      Enum.reduce(all_extend_blocks, parsed_top_messages, fn {:extend, [target_name | field_asts]},
                                                             acc_msgs ->
        parsed_fields =
          Enum.map(field_asts, fn
            {:field, _} = f -> Field.parse(f)
            _ -> nil
          end)
          |> Enum.reject(&is_nil/1)

        apply_extend_to_messages(acc_msgs, target_name, parsed_fields, package)
      end)

    messages =
      Enum.flat_map(top_messages, &flatten_message_entries/1)
      |> Map.new()

    nested_enums_from_messages =
      Enum.flat_map(top_messages, &flatten_nested_enums/1)

    top_enums =
      ast
      |> Enum.filter(fn
        {:enum, _} -> true
        _ -> false
      end)
      |> Enum.flat_map(fn e_ast ->
        enum = SchemaEnum.parse(e_ast)
        full_name = if package && package != "", do: "#{package}.#{enum.name}", else: enum.name

        if full_name != enum.name do
          [{enum.name, enum}, {full_name, enum}]
        else
          [{enum.name, enum}]
        end
      end)

    enums = Map.new(top_enums ++ nested_enums_from_messages)

    options =
      Enum.reduce(ast, %{}, fn
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    features = Field.extract_features(options, default_features_for_syntax(syntax, edition))

    %__MODULE__{
      package: package,
      syntax: syntax,
      edition: edition,
      features: features,
      services: services,
      messages: messages,
      enums: enums,
      options: options,
      imports: imports,
      files: []
    }
    |> finalize_schema()
  end

  defp collect_message_extend_blocks(%Message{extend_blocks: eb, nested_messages: nm}) do
    eb ++ Enum.flat_map(nm, &collect_message_extend_blocks/1)
  end

  defp apply_extend_to_messages(messages, target_name, new_fields, package) do
    Enum.map(messages, fn msg ->
      matches =
        msg.name == target_name or
          msg.full_name == target_name or
          (package && "#{package}.#{msg.name}" == target_name) or
          String.ends_with?(msg.full_name || "", ".#{target_name}") or
          String.ends_with?(target_name, ".#{msg.name}")

      if matches do
        updated_fields = msg.fields ++ new_fields

        updated_by_num =
          new_fields
          |> Enum.filter(&is_integer(&1.number))
          |> Enum.reduce(msg.fields_by_number, fn f, acc -> Map.put(acc, f.number, f) end)

        updated_by_name =
          new_fields
          |> Enum.filter(&is_atom(&1.name))
          |> Enum.reduce(msg.fields_by_name, fn f, acc -> Map.put(acc, f.name, f) end)

        %{
          msg
          | fields: updated_fields,
            fields_by_number: updated_by_num,
            fields_by_name: updated_by_name
        }
      else
        updated_nested =
          apply_extend_to_messages(msg.nested_messages, target_name, new_fields, package)

        %{msg | nested_messages: updated_nested}
      end
    end)
  end

  @doc """
  Links message submessage_desc and enum_desc on field descriptors and builds fast default_map.
  """
  def finalize_schema(%__MODULE__{messages: messages} = schema) do
    file_features =
      Field.extract_features(
        schema.options,
        default_features_for_syntax(schema.syntax, schema.edition)
      )

    linked_messages =
      Enum.map(messages, fn {name, msg} ->
        msg_features = Field.extract_features(msg.options, file_features)

        linked_fields =
          Enum.map(msg.fields, fn
            %Field{kind: :oneof, fields: subfields} = oneof_f ->
              subfield_names = Enum.map(subfields, & &1.name)

              linked_sub =
                Enum.map(subfields, fn f ->
                  field_features = Field.extract_features(f.options, msg_features)
                  siblings = List.delete(subfield_names, f.name)

                  f
                  |> Map.put(:oneof_siblings, siblings)
                  |> Map.put(:features, field_features)
                  |> apply_features_to_field(schema.syntax)
                  |> link_field_descriptors(schema)
                end)

              %{oneof_f | fields: linked_sub, features: msg_features}

            %Field{} = f ->
              field_features = Field.extract_features(f.options, msg_features)

              f
              |> Map.put(:features, field_features)
              |> apply_features_to_field(schema.syntax)
              |> link_field_descriptors(schema)
          end)

        fbn =
          linked_fields
          |> Enum.flat_map(fn
            %Field{kind: :oneof, fields: subs} -> Enum.map(subs, &{&1.number, &1})
            %Field{number: num} = f when is_integer(num) -> [{num, f}]
            _ -> []
          end)
          |> Map.new()

        fbn_name =
          linked_fields
          |> Enum.flat_map(fn
            %Field{kind: :oneof, fields: subs} -> Enum.map(subs, &{&1.name, &1})
            %Field{name: fname} = f when is_atom(fname) -> [{fname, f}]
            _ -> []
          end)
          |> Map.new()

        default_map =
          Enum.reduce(linked_fields, %{}, fn field, acc ->
            case field.kind do
              :oneof ->
                Enum.reduce(field.fields, acc, fn subfield, inner ->
                  Map.put(inner, subfield.name, nil)
                end)

              :map ->
                Map.put(acc, field.name, %{})

              _ ->
                if field.prefix == :repeated do
                  Map.put(acc, field.name, [])
                else
                  if field.prefix == :required do
                    Map.put(acc, field.name, nil)
                  else
                    Map.put(acc, field.name, field.default_value)
                  end
                end
            end
          end)

        repeated_fields =
          linked_fields
          |> Enum.filter(fn f -> f.prefix == :repeated end)
          |> Enum.map(& &1.name)

        required_fields =
          linked_fields
          |> Enum.filter(fn f -> f.prefix == :required end)
          |> Enum.map(& &1.name)

        {name,
         %{
           msg
           | fields: linked_fields,
             fields_by_number: fbn,
             fields_by_name: fbn_name,
             default_map: default_map,
             repeated_fields: repeated_fields,
             required_fields: required_fields,
             features: msg_features
         }}
      end)
      |> Map.new()

    finalized = %{schema | messages: linked_messages, features: file_features}
    SimpleGrpc.Compiler.compile_schema(finalized)
  end

  def default_features_for_syntax(syntax, _edition) do
    case syntax do
      :editions ->
        %{
          field_presence: :explicit,
          repeated_field_encoding: :packed,
          enum_type: :open,
          utf8_validation: :verify,
          message_encoding: :length_prefixed,
          json_format: :allow
        }

      :proto2 ->
        %{
          field_presence: :explicit,
          repeated_field_encoding: :expanded,
          enum_type: :closed,
          utf8_validation: :none
        }

      _ ->
        %{
          field_presence: :implicit,
          repeated_field_encoding: :packed,
          enum_type: :open,
          utf8_validation: :verify
        }
    end
  end

  defp apply_features_to_field(%Field{kind: :field} = f, syntax) do
    has_explicit_presence_option =
      Map.has_key?(f.options, "features.field_presence") or
        Map.has_key?(f.options, "(features.field_presence)") or
        Map.has_key?(f.options, :features_field_presence)

    prefix =
      cond do
        f.prefix == :repeated ->
          :repeated

        syntax == :editions ->
          case f.features[:field_presence] do
            :legacy_required -> :required
            :implicit -> :implicit
            :explicit -> :optional
            _ -> :optional
          end

        has_explicit_presence_option and f.features[:field_presence] == :legacy_required ->
          :required

        has_explicit_presence_option and f.features[:field_presence] == :implicit ->
          :implicit

        has_explicit_presence_option and f.features[:field_presence] == :explicit ->
          :optional

        true ->
          f.prefix
      end

    is_packable =
      case Map.get(f.options, :packed) do
        nil ->
          case f.features[:repeated_field_encoding] do
            :expanded ->
              false

            :packed ->
              Field.compute_is_packable(f.type)

            _ ->
              if syntax == :proto2 do
                false
              else
                Field.compute_is_packable(f.type)
              end
          end

        val ->
          val == true or val == "true"
      end

    %{f | prefix: prefix, is_packable: is_packable}
  end

  defp apply_features_to_field(f, _syntax), do: f

  defp link_field_descriptors(%Field{type: type, number: num} = field, schema)
       when is_binary(type) do
    case field_wire_type(type) do
      nil ->
        sub_desc = message(schema, type)
        enum_desc = enum(schema, type)

        wire_type =
          cond do
            enum_desc != nil -> 0
            sub_desc != nil -> 2
            true -> nil
          end

        tag_bytes =
          if is_integer(num) and is_integer(wire_type), do: encode_tag(num, wire_type), else: nil

        is_packable =
          cond do
            Map.has_key?(field.options, :packed) ->
              field.options[:packed] in [true, "true"]

            field.features[:repeated_field_encoding] == :expanded ->
              false

            field.features[:repeated_field_encoding] == :packed and enum_desc != nil ->
              true

            schema.syntax == :proto2 ->
              false

            enum_desc != nil ->
              true

            true ->
              false
          end

        default_val =
          cond do
            enum_desc != nil ->
              if Map.has_key?(field.options, :default) or Map.has_key?(field.options, "default") do
                resolve_enum_default(field.default_value, enum_desc)
              else
                if field.prefix == :optional do
                  if schema.syntax == :editions, do: enum_desc.zero_atom || 0, else: nil
                else
                  enum_desc.zero_atom || 0
                end
              end

            sub_desc != nil ->
              nil

            true ->
              nil
          end

        %{
          field
          | submessage_desc: sub_desc,
            enum_desc: enum_desc,
            wire_type: wire_type,
            tag_bytes: tag_bytes,
            is_packable: is_packable,
            default_value: default_val
        }

      wire_type ->
        tag_bytes = if is_integer(num), do: encode_tag(num, wire_type), else: nil

        is_packable =
          cond do
            Map.has_key?(field.options, :packed) ->
              field.options[:packed] in [true, "true"]

            field.features[:repeated_field_encoding] == :expanded ->
              false

            field.features[:repeated_field_encoding] == :packed ->
              is_packable_type(type)

            schema.syntax == :proto2 ->
              false

            true ->
              is_packable_type(type)
          end

        default_val =
          cond do
            Map.has_key?(field.options, :default) or Map.has_key?(field.options, "default") ->
              field.default_value

            schema.syntax == :editions and field.prefix == :optional ->
              default_scalar_type(type)

            field.prefix == :optional ->
              nil

            field.prefix == :required ->
              nil

            true ->
              default_scalar_type(type)
          end

        %{
          field
          | wire_type: wire_type,
            tag_bytes: tag_bytes,
            is_packable: is_packable,
            default_value: default_val
        }
    end
  end

  defp link_field_descriptors(%Field{number: num, kind: :map} = field, _schema) do
    tag_bytes = if is_integer(num), do: encode_tag(num, 2), else: nil
    %{field | wire_type: 2, tag_bytes: tag_bytes, is_packable: false, default_value: %{}}
  end

  defp link_field_descriptors(field, _schema), do: field

  defp resolve_enum_default(val, %SchemaEnum{
         atom_to_int: a2i,
         str_to_int: s2i,
         zero_atom: zero,
         fields: fields
       }) do
    cond do
      is_atom(val) and Map.has_key?(a2i, val) ->
        val

      is_binary(val) and Map.has_key?(s2i, val) ->
        int_v = Map.get(s2i, val)
        Enum.find_value(fields, fn {k, v} -> if v == int_v, do: k end) || String.to_atom(val)

      is_integer(val) ->
        Enum.find_value(fields, fn {k, v} -> if v == val, do: k end) || val

      true ->
        zero || val
    end
  end

  defp resolve_enum_default(val, _), do: val

  defp encode_tag(field_num, wire_type) when is_integer(field_num) and is_integer(wire_type) do
    encode_varint_bin(field_num <<< 3 ||| wire_type)
  end

  defp encode_varint_bin(n) when is_integer(n) and n >= 0 and n <= 127, do: <<n>>

  defp encode_varint_bin(n) when is_integer(n) and n >= 0 do
    <<1::1, n &&& 0x7F::7, encode_varint_bin(n >>> 7)::binary>>
  end

  defp is_packable_type("string"), do: false
  defp is_packable_type("bytes"), do: false
  defp is_packable_type(_), do: true

  defp default_scalar_type("string"), do: ""
  defp default_scalar_type("bytes"), do: <<>>
  defp default_scalar_type("bool"), do: false
  defp default_scalar_type("float"), do: 0.0
  defp default_scalar_type("double"), do: 0.0
  defp default_scalar_type(_int_type), do: 0

  defp field_wire_type("int32"), do: 0
  defp field_wire_type("int64"), do: 0
  defp field_wire_type("uint32"), do: 0
  defp field_wire_type("uint64"), do: 0
  defp field_wire_type("sint32"), do: 0
  defp field_wire_type("sint64"), do: 0
  defp field_wire_type("bool"), do: 0
  defp field_wire_type("fixed64"), do: 1
  defp field_wire_type("sfixed64"), do: 1
  defp field_wire_type("double"), do: 1
  defp field_wire_type("string"), do: 2
  defp field_wire_type("bytes"), do: 2
  defp field_wire_type("fixed32"), do: 5
  defp field_wire_type("sfixed32"), do: 5
  defp field_wire_type("float"), do: 5
  defp field_wire_type(_), do: nil

  @doc """
  Finds a service by name or full name.
  """
  def service(%__MODULE__{services: services, package: package}, name) do
    name_str = name |> to_string() |> String.trim_leading(".")

    Map.get(services, name_str) ||
      (package && Map.get(services, String.replace_prefix(name_str, "#{package}.", ""))) ||
      Enum.find_value(services, fn {_k, s} ->
        if s.full_name == name_str or s.name == name_str or
             String.ends_with?(s.full_name || "", ".#{name_str}"),
           do: s
      end)
  end

  @doc """
  Finds a message by name or full name across top-level messages and package namespaces.
  """
  def message(%__MODULE__{messages: messages, package: package}, name) do
    name_str = name |> to_string() |> String.trim_leading(".")

    Map.get(messages, name_str) ||
      (package && Map.get(messages, "#{package}.#{name_str}")) ||
      (package && Map.get(messages, String.replace_prefix(name_str, "#{package}.", ""))) ||
      Enum.find_value(messages, fn {_k, m} ->
        if m.full_name == name_str or m.name == name_str or
             String.ends_with?(m.full_name || "", ".#{name_str}") or
             String.ends_with?(name_str, ".#{m.name}"),
           do: m
      end)
  end

  @doc """
  Finds an enum by name or full name across top-level enums, package namespaces, and nested message enums.
  """
  def enum(%__MODULE__{enums: enums, messages: messages, package: package}, name) do
    name_str = name |> to_string() |> String.trim_leading(".")
    name_atom = if is_atom(name), do: name, else: String.to_atom(name_str)

    Map.get(enums, name) ||
      Map.get(enums, name_str) ||
      Map.get(enums, name_atom) ||
      (package && Map.get(enums, "#{package}.#{name_str}")) ||
      (package && Map.get(enums, String.replace_prefix(name_str, "#{package}.", ""))) ||
      (package &&
         Map.get(
           enums,
           String.to_atom(String.replace_prefix(name_str, "#{package}.", ""))
         )) ||
      Enum.find_value(enums, fn {_k, e} ->
        if e.name == name_str or to_string(e.name) == name_str or
             (is_struct(e, SchemaEnum) and
                ("#{package}.#{e.name}" == name_str or
                   String.ends_with?(name_str, ".#{e.name}"))),
           do: e
      end) ||
      Enum.find_value(messages, fn {_msg_name, msg} ->
        find_nested_enum(msg, name_str)
      end)
  end

  def enum(_other, _name), do: nil

  defp find_nested_enum(
         %Message{name: msg_name, nested_enums: nested_enums, nested_messages: nested_messages},
         name_str
       ) do
    name_str = String.trim_leading(name_str, ".")

    Enum.find(nested_enums, fn e ->
      e.name == name_str or
        "#{msg_name}.#{e.name}" == name_str or
        String.ends_with?(name_str, ".#{e.name}")
    end) ||
      Enum.find_value(nested_messages, &find_nested_enum(&1, name_str))
  end

  @doc """
  Finds an RPC across all services in the schema by its gRPC path.
  """
  def find_rpc(%__MODULE__{services: services}, path) when is_binary(path) do
    Enum.find_value(services, fn {_s_name, service} ->
      Enum.find_value(service.rpcs, fn {_r_name, rpc} ->
        if rpc.path == path do
          {service, rpc}
        end
      end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Internal Import Resolution
  # ---------------------------------------------------------------------------

  defp load_file_with_imports(file_path, follow_imports, import_paths, visited, opts) do
    case SimpleGrpc.Parser.parse(file_path) do
      {:ok, {:editions, edition}, ast} ->
        base_schema = from_ast(ast, syntax: :editions, edition: edition)
        base_schema = %{base_schema | files: [Path.expand(file_path)]}

        if follow_imports and base_schema.imports != [] do
          resolve_and_merge_imports(
            base_schema,
            file_path,
            import_paths,
            visited,
            opts
          )
        else
          {:ok, base_schema}
        end

      {:ok, syntax, ast} ->
        base_schema = from_ast(ast, syntax: syntax)
        base_schema = %{base_schema | files: [Path.expand(file_path)]}

        if follow_imports and base_schema.imports != [] do
          resolve_and_merge_imports(
            base_schema,
            file_path,
            import_paths,
            visited,
            opts
          )
        else
          {:ok, base_schema}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_and_merge_imports(schema, current_file, import_paths, visited, opts) do
    Enum.reduce_while(schema.imports, {:ok, schema}, fn import_path, {:ok, current_schema} ->
      case find_import_file(import_path, current_file, import_paths) do
        {:ok, resolved_path} ->
          expanded = Path.expand(resolved_path)

          if MapSet.member?(visited, expanded) do
            {:cont, {:ok, current_schema}}
          else
            new_visited = MapSet.put(visited, expanded)

            case load_file_with_imports(
                   resolved_path,
                   true,
                   import_paths,
                   new_visited,
                   opts
                 ) do
              {:ok, imported_schema} ->
                {:cont, {:ok, merge(current_schema, imported_schema)}}

              {:error, reason} ->
                {:halt, {:error, "Failed to import #{import_path}: #{reason}"}}
            end
          end

        :error ->
          case SimpleGrpc.Schema.WellKnownTypes.schema_for(import_path) do
            %SimpleGrpc.Schema{} = wkt_schema ->
              {:cont, {:ok, merge(current_schema, wkt_schema)}}

            nil ->
              {:cont, {:ok, current_schema}}
          end
      end
    end)
  end

  defp find_import_file(import_path, current_file, import_paths) do
    search_dirs =
      [
        Path.dirname(current_file)
        | import_paths ++ bundled_protos_dirs()
      ]
      |> Enum.uniq()

    Enum.find_value(search_dirs, :error, fn dir ->
      candidate = Path.expand(import_path, dir)

      if File.regular?(candidate) do
        {:ok, candidate}
      else
        nil
      end
    end)
  end

  defp bundled_protos_dirs do
    dirs =
      case :code.priv_dir(:simple_grpc) do
        {:error, _} ->
          []

        priv_dir when is_list(priv_dir) or is_binary(priv_dir) ->
          path = to_string(priv_dir)
          [path, Path.join(path, "protos"), Path.join(path, "proto")]
      end

    ["priv/protos", "priv", "protos" | dirs]
    |> Enum.filter(&File.dir?/1)
    |> Enum.uniq()
  end

  # ---------------------------------------------------------------------------
  # Service Dependency Graph Traversal
  # ---------------------------------------------------------------------------

  defp collect_service_dependencies(schema, %Service{rpcs: rpcs}) do
    root_type_names =
      Enum.flat_map(rpcs, fn {_name, rpc} ->
        [rpc.input, rpc.output]
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    traverse_type_dependencies(root_type_names, schema, MapSet.new(), %{}, %{})
  end

  defp traverse_type_dependencies([], _schema, _visited, acc_messages, acc_enums) do
    {Map.values(acc_messages), Map.values(acc_enums)}
  end

  defp traverse_type_dependencies(
         [type_name | rest_types],
         schema,
         visited,
         acc_messages,
         acc_enums
       ) do
    clean_type = type_name |> to_string() |> String.trim_leading(".")

    if MapSet.member?(visited, clean_type) do
      traverse_type_dependencies(rest_types, schema, visited, acc_messages, acc_enums)
    else
      visited = MapSet.put(visited, clean_type)

      msg = message(schema, clean_type)
      enum_val = enum(schema, clean_type)

      cond do
        msg != nil ->
          acc_messages = Map.put(acc_messages, msg.full_name || msg.name, msg)

          # Extract referenced types from message fields and nested structures
          referenced = extract_referenced_types(msg)

          traverse_type_dependencies(
            rest_types ++ referenced,
            schema,
            visited,
            acc_messages,
            acc_enums
          )

        enum_val != nil ->
          enum_name =
            if is_struct(enum_val, SchemaEnum), do: enum_val.name, else: to_string(clean_type)

          acc_enums = Map.put(acc_enums, enum_name, enum_val)
          traverse_type_dependencies(rest_types, schema, visited, acc_messages, acc_enums)

        true ->
          traverse_type_dependencies(rest_types, schema, visited, acc_messages, acc_enums)
      end
    end
  end

  defp extract_referenced_types(%Message{
         fields: fields,
         nested_messages: nested_messages,
         nested_enums: _nested_enums
       }) do
    field_types =
      Enum.flat_map(fields, fn
        %Field{kind: :map, key_type: k, val_type: v} -> [k, v]
        %Field{kind: :oneof, fields: subfields} -> Enum.map(subfields, & &1.type)
        %Field{type: t} when is_binary(t) -> [t]
        _ -> []
      end)

    nested_types = Enum.flat_map(nested_messages, &extract_referenced_types/1)

    (field_types ++ nested_types)
    |> Enum.reject(
      &(&1 in ~w(int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64 float double bool string bytes))
    )
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
end
