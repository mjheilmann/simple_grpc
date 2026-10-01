defmodule SimpleGrpc.Schema do
  @moduledoc """
  Represents a complete protobuf schema containing services, messages, and enums as runtime metadata.
  """

  alias SimpleGrpc.Schema.{Service, Message, Field}
  alias SimpleGrpc.Schema.Enum, as: SchemaEnum

  defstruct [
    :package,
    syntax: :proto3,
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
      syntax: s2.syntax || s1.syntax || :proto3,
      services: Map.merge(s1.services, s2.services),
      messages: Map.merge(s1.messages, s2.messages),
      enums: Map.merge(s1.enums, s2.enums),
      options: Map.merge(s1.options, s2.options),
      imports: Enum.uniq(s1.imports ++ s2.imports),
      files: Enum.uniq(s1.files ++ s2.files)
    }
  end

  @doc """
  Builds a Schema struct from a proto string.
  """
  def from_string(proto_string, opts \\ []) do
    syntax = Keyword.get(opts, :syntax, :proto3)

    case SimpleGrpc.Parser.Proto3.parse(proto_string) do
      {:ok, ast, "", %{}, _, _} ->
        {:ok, from_ast(ast, syntax: syntax)}

      {:ok, _ast, rest, _, _, _} ->
        {:error, "incomplete parse, remaining: #{inspect(rest)}"}

      err ->
        {:error, "failed to parse proto string: #{inspect(err)}"}
    end
  end

  @doc """
  Builds a Schema struct from parser AST.
  """
  def from_ast(ast, opts \\ []) when is_list(ast) do
    syntax = Keyword.get(opts, :syntax, :proto3)

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

    messages =
      ast
      |> Enum.filter(fn
        {:message, _} -> true
        _ -> false
      end)
      |> Enum.flat_map(fn m_ast ->
        msg = Message.parse(m_ast)
        full_name = if package && package != "", do: "#{package}.#{msg.name}", else: msg.name
        msg = %{msg | full_name: full_name}

        if full_name != msg.name do
          [{msg.name, msg}, {full_name, msg}]
        else
          [{msg.name, msg}]
        end
      end)
      |> Map.new()

    enums =
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
      |> Map.new()

    options =
      Enum.reduce(ast, %{}, fn
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    %__MODULE__{
      package: package,
      syntax: syntax,
      services: services,
      messages: messages,
      enums: enums,
      options: options,
      imports: imports,
      files: []
    }
  end

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

  defp traverse_type_dependencies([type_name | rest_types], schema, visited, acc_messages, acc_enums) do
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
          traverse_type_dependencies(rest_types ++ referenced, schema, visited, acc_messages, acc_enums)

        enum_val != nil ->
          enum_name = if is_struct(enum_val, SchemaEnum), do: enum_val.name, else: to_string(clean_type)
          acc_enums = Map.put(acc_enums, enum_name, enum_val)
          traverse_type_dependencies(rest_types, schema, visited, acc_messages, acc_enums)

        true ->
          traverse_type_dependencies(rest_types, schema, visited, acc_messages, acc_enums)
      end
    end
  end

  defp extract_referenced_types(%Message{fields: fields, nested_messages: nested_messages, nested_enums: _nested_enums}) do
    field_types =
      Enum.flat_map(fields, fn
        %Field{kind: :map, key_type: k, val_type: v} -> [k, v]
        %Field{kind: :oneof, fields: subfields} -> Enum.map(subfields, & &1.type)
        %Field{type: t} when is_binary(t) -> [t]
        _ -> []
      end)

    nested_types = Enum.flat_map(nested_messages, &extract_referenced_types/1)

    (field_types ++ nested_types)
    |> Enum.reject(&(&1 in ~w(int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64 float double bool string bytes)))
  end
end
