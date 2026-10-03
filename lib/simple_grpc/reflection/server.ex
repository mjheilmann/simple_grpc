defmodule SimpleGrpc.Reflection.Server do
  @moduledoc """
  Implements the standard gRPC Server Reflection Protocol (`grpc.reflection.v1` and `grpc.reflection.v1alpha`).
  Serves descriptor metadata directly from memory without reading `.proto` files on disk.
  """

  alias SimpleGrpc.Registry
  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.WellKnownTypes
  alias SimpleGrpc.Reflection.Descriptor

  @v1_service "grpc.reflection.v1.ServerReflection"
  @v1alpha_service "grpc.reflection.v1alpha.ServerReflection"

  @doc """
  Registers the reflection service (`v1` and `v1alpha`) in `SimpleGrpc.Registry`.
  Options:
  - `:only` - list of service names or module atoms to reflect
  - `:except` - list of service names or module atoms to exclude
  - `:services` - list of specific services or modules to reflect
  - `:schemas` - list of `%SimpleGrpc.Schema{}` structs to reflect
  """
  def register(opts \\ []) do
    v1_schema = reflection_v1_schema()
    v1alpha_schema = reflection_v1alpha_schema()

    handler = fn req, stream ->
      server_reflection_info(req, stream, opts)
    end

    Registry.register_service(v1_schema, handler)
    Registry.register_service(v1alpha_schema, handler)
    :ok
  end

  @doc """
  Builds the route map for reflection services (`v1` and `v1alpha`) with the given options.
  """
  def build_reflection_routes(opts \\ []) do
    v1_schema = reflection_v1_schema()
    v1alpha_schema = reflection_v1alpha_schema()

    handler = fn req, stream ->
      server_reflection_info(req, stream, opts)
    end

    {:ok, v1_routes} = Registry.generate_routes(v1_schema, handler)
    {:ok, v1alpha_routes} = Registry.generate_routes(v1alpha_schema, handler)

    Map.merge(v1_routes, v1alpha_routes)
  end

  @doc """
  Handles `ServerReflectionInfo` requests from `grpcurl`, BloomRPC, Postman, etc.
  """
  def server_reflection_info(req, stream \\ nil, opts \\ [])

  def server_reflection_info(req, _stream, opts) do
    cond do
      is_struct(req, Stream) or is_function(req, 2) or (is_list(req) and not Keyword.keyword?(req)) ->
        Stream.map(req, fn single_req ->
          {:ok, resp} = process_reflection_request(single_req, opts)
          resp
        end)

      is_map(req) or (is_list(req) and Keyword.keyword?(req)) ->
        process_reflection_request(req, opts)

      true ->
        {:error, 3, "Invalid reflection request"}
    end
  end

  defp process_reflection_request(req, opts) do
    req_map = normalize_keys(req)
    host = Map.get(req_map, :host, "")

    response_body =
      cond do
        (symbol = Map.get(req_map, :file_containing_symbol)) && symbol != "" ->
          handle_file_containing_symbol(symbol, opts)

        (filename = Map.get(req_map, :file_by_filename)) && filename != "" ->
          handle_file_by_filename(filename, opts)

        (ext_type = Map.get(req_map, :all_extension_numbers_of_type)) && ext_type != "" ->
          %{all_extension_numbers_response: %{base_type_name: ext_type, extension_number: []}}

        Map.get(req_map, :file_containing_extension) != nil ->
          %{error_response: %{error_code: 5, error_message: "Extensions not found"}}

        is_binary(Map.get(req_map, :list_services)) ->
          handle_list_services(opts)

        true ->
          %{error_response: %{error_code: 3, error_message: "Invalid reflection request"}}
      end

    full_response =
      response_body
      |> Map.put(:valid_host, host)
      |> Map.put(:original_request, req)

    {:ok, full_response}
  end

  # ---------------------------------------------------------------------------
  # Request Handlers
  # ---------------------------------------------------------------------------

  defp handle_list_services(opts) do
    registered =
      case Keyword.get(opts, :services) do
        nil ->
          case Keyword.get(opts, :schemas) do
            nil ->
              Registry.list_services()

            schemas when is_list(schemas) ->
              Enum.flat_map(schemas, fn s ->
                Map.values(s.services) |> Enum.map(& &1.full_name)
              end)
          end

        services when is_list(services) ->
          Enum.flat_map(services, fn
            mod when is_atom(mod) ->
              if function_exported?(mod, :__schema__, 0) do
                Map.values(mod.__schema__().services) |> Enum.map(& &1.full_name)
              else
                [to_string(mod)]
              end

            %Schema{} = s ->
              Map.values(s.services) |> Enum.map(& &1.full_name)

            {mod_or_s, _handlers} ->
              cond do
                is_atom(mod_or_s) && function_exported?(mod_or_s, :__schema__, 0) ->
                  Map.values(mod_or_s.__schema__().services) |> Enum.map(& &1.full_name)

                is_struct(mod_or_s, Schema) ->
                  Map.values(mod_or_s.services) |> Enum.map(& &1.full_name)

                true ->
                  [to_string(mod_or_s)]
              end

            name when is_binary(name) ->
              [name]
          end)
      end

    filtered =
      registered
      |> maybe_filter_only(Keyword.get(opts, :only))
      |> maybe_filter_except(Keyword.get(opts, :except))

    all_services =
      (filtered ++ [@v1_service, @v1alpha_service])
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.sort()

    service_entries = Enum.map(all_services, fn name -> %{name: name} end)

    %{list_services_response: %{service: service_entries}}
  end

  defp handle_file_containing_symbol(symbol, opts) when is_binary(symbol) do
    clean_sym = String.trim_leading(symbol, ".")

    case find_schema_and_dependencies_for_symbol(clean_sym, opts) do
      {:ok, schemas} ->
        fdp_bytes_list =
          schemas
          |> Enum.map(&Descriptor.encode_file_descriptor/1)
          |> Enum.reject(&is_nil/1)

        %{file_descriptor_response: %{file_descriptor_proto: fdp_bytes_list}}

      :not_found ->
        %{
          error_response: %{
            error_code: 5,
            error_message: "File containing symbol #{inspect(symbol)} not found"
          }
        }
    end
  end

  defp handle_file_by_filename(filename, opts) when is_binary(filename) do
    case find_schema_by_filename(filename, opts) do
      {:ok, schemas} ->
        fdp_bytes_list =
          schemas
          |> Enum.map(fn schema ->
            Descriptor.encode_file_descriptor(schema, filename)
          end)
          |> Enum.reject(&is_nil/1)

        %{file_descriptor_response: %{file_descriptor_proto: fdp_bytes_list}}

      :not_found ->
        %{
          error_response: %{
            error_code: 5,
            error_message: "File not found: #{inspect(filename)}"
          }
        }
    end
  end

  # ---------------------------------------------------------------------------
  # Symbol and Schema Lookups
  # ---------------------------------------------------------------------------

  defp find_schema_and_dependencies_for_symbol(symbol, opts) do
    user_schemas = get_schemas_for_opts(opts)
    wkt_unified = WellKnownTypes.unified_schema()
    v1_schema = reflection_v1_schema()
    v1alpha_schema = reflection_v1alpha_schema()

    all_candidate_schemas = user_schemas ++ [v1_schema, v1alpha_schema, wkt_unified]

    found_schema =
      Enum.find(all_candidate_schemas, fn schema ->
        schema_contains_symbol?(schema, symbol)
      end)

    if found_schema && symbol_allowed?(symbol, found_schema, opts) do
      deps = collect_dependent_schemas(found_schema, all_candidate_schemas)
      {:ok, [found_schema | deps] |> Enum.uniq()}
    else
      :not_found
    end
  end

  defp find_schema_by_filename(filename, opts) do
    user_schemas = get_schemas_for_opts(opts)
    base_name = Path.basename(filename)

    matching_schema =
      Enum.find(user_schemas, fn schema ->
        Enum.any?(schema.files, fn f ->
          Path.basename(f) == base_name or f == filename or String.ends_with?(f, filename)
        end) or
          schema.package == Path.rootname(base_name) or
          "#{schema.package}.proto" == base_name
      end)

    cond do
      matching_schema != nil && schema_allowed?(matching_schema, opts) ->
        deps = collect_dependent_schemas(matching_schema, user_schemas)
        {:ok, [matching_schema | deps] |> Enum.uniq()}

      WellKnownTypes.schema_for(filename) != nil ->
        {:ok, [WellKnownTypes.schema_for(filename)]}

      true ->
        :not_found
    end
  end

  defp get_schemas_for_opts(opts) do
    case Keyword.get(opts, :schemas) do
      nil ->
        case Keyword.get(opts, :services) do
          nil ->
            Registry.list_schemas()

          services when is_list(services) ->
            Enum.flat_map(services, fn
              mod when is_atom(mod) ->
                if function_exported?(mod, :__schema__, 0), do: [mod.__schema__()], else: []

              %Schema{} = s ->
                [s]

              {mod_or_s, _handlers} ->
                cond do
                  is_atom(mod_or_s) && function_exported?(mod_or_s, :__schema__, 0) ->
                    [mod_or_s.__schema__()]

                  is_struct(mod_or_s, Schema) ->
                    [mod_or_s]

                  true ->
                    []
                end

              _ ->
                []
            end)
        end

      schemas when is_list(schemas) ->
        schemas
    end
  end

  defp symbol_allowed?(symbol, schema, opts) do
    only = Keyword.get(opts, :only)
    except = Keyword.get(opts, :except)

    cond do
      schema.package in ["grpc.reflection.v1", "grpc.reflection.v1alpha", "google.protobuf"] ->
        true

      only != nil ->
        allowed = normalize_filter_list(only)
        Enum.any?(allowed, &(&1 == symbol or schema_matches_name?(schema, &1)))

      except != nil ->
        disallowed = normalize_filter_list(except)
        not Enum.any?(disallowed, &(&1 == symbol or schema_matches_name?(schema, &1)))

      true ->
        true
    end
  end

  defp schema_allowed?(schema, opts) do
    only = Keyword.get(opts, :only)
    except = Keyword.get(opts, :except)

    cond do
      schema.package in ["grpc.reflection.v1", "grpc.reflection.v1alpha", "google.protobuf"] ->
        true

      only != nil ->
        allowed = normalize_filter_list(only)
        Enum.any?(allowed, &schema_matches_name?(schema, &1))

      except != nil ->
        disallowed = normalize_filter_list(except)
        not Enum.any?(disallowed, &schema_matches_name?(schema, &1))

      true ->
        true
    end
  end

  defp schema_matches_name?(%Schema{services: services, package: package}, name) do
    clean = String.trim_leading(to_string(name), ".")

    package == clean or
      Map.has_key?(services, clean) or
      (package && Map.has_key?(services, String.replace_prefix(clean, "#{package}.", ""))) or
      Enum.any?(services, fn {_k, s} -> s.name == clean or s.full_name == clean end)
  end

  defp maybe_filter_only(list, nil), do: list

  defp maybe_filter_only(list, only) when is_list(only) do
    allowed = normalize_filter_list(only)
    Enum.filter(list, &(&1 in allowed))
  end

  defp maybe_filter_except(list, nil), do: list

  defp maybe_filter_except(list, except) when is_list(except) do
    disallowed = normalize_filter_list(except)
    Enum.reject(list, &(&1 in disallowed))
  end

  defp normalize_filter_list(items) when is_list(items) do
    Enum.map(items, fn
      mod when is_atom(mod) ->
        if function_exported?(mod, :__grpc_service_prefix__, 0),
          do: mod.__grpc_service_prefix__(),
          else: to_string(mod)

      name when is_binary(name) ->
        String.trim_leading(name, ".")
    end)
  end

  defp schema_contains_symbol?(%Schema{services: services, messages: messages, enums: enums, package: package}, symbol) do
    clean_sym = String.trim_leading(symbol, ".")

    # 1. Match service
    service_match =
      Map.has_key?(services, clean_sym) or
        (package != nil and
           Map.has_key?(services, String.replace_prefix(clean_sym, "#{package}.", ""))) or
        Enum.any?(services, fn {_k, s} -> s.name == clean_sym or s.full_name == clean_sym end)

    # 2. Match message
    message_match =
      Map.has_key?(messages, clean_sym) or
        (package != nil and
           Map.has_key?(messages, String.replace_prefix(clean_sym, "#{package}.", ""))) or
        Enum.any?(messages, fn {_k, m} -> m.name == clean_sym or m.full_name == clean_sym end)

    # 3. Match enum
    enum_match =
      Map.has_key?(enums, clean_sym) or
        (package != nil and
           Map.has_key?(enums, String.replace_prefix(clean_sym, "#{package}.", "")))

    # 4. Match method (e.g. "package.Service.Method")
    method_match =
      Enum.any?(services, fn {_s_name, s} ->
        Enum.any?(s.rpcs, fn {_r_name, rpc} ->
          "#{s.full_name}.#{rpc.name}" == clean_sym or "#{s.name}.#{rpc.name}" == clean_sym
        end)
      end)

    service_match or message_match or enum_match or method_match
  end

  defp collect_dependent_schemas(%Schema{} = primary_schema, all_schemas) do
    # Find all type names referenced in primary schema
    referenced_types =
      Enum.flat_map(primary_schema.messages, fn {_name, msg} ->
        Enum.flat_map(msg.fields, fn f ->
          [f.type, f.key_type, f.val_type]
        end)
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    Enum.filter(all_schemas, fn schema ->
      schema != primary_schema and
        Enum.any?(referenced_types, fn type ->
          schema_contains_symbol?(schema, type)
        end)
    end)
  end

  defp normalize_keys(map) when is_map(map) do
    Enum.reduce(map, %{}, fn {k, v}, acc ->
      atom_key = if is_binary(k), do: String.to_atom(k), else: k
      Map.put(acc, atom_key, v)
    end)
  end

  # ---------------------------------------------------------------------------
  # Pre-compiled Reflection Schemas (v1 and v1alpha)
  # ---------------------------------------------------------------------------

  def reflection_v1_schema do
    {:ok, schema} = Schema.from_file("priv/protos/grpc/reflection/v1/reflection.proto")
    schema
  end

  def reflection_v1alpha_schema do
    {:ok, schema} = Schema.from_file("priv/protos/grpc/reflection/v1alpha/reflection.proto")
    schema
  end
end
