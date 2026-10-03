defmodule SimpleGrpc.Client.Reflection do
  @moduledoc """
  gRPC Server Reflection client supporting `grpc.reflection.v1` and `grpc.reflection.v1alpha`.

  Allows querying remote gRPC servers for registered services, methods, and types,
  fetching binary `FileDescriptorProto` descriptors, and converting them into
  `%SimpleGrpc.Schema{}` structs dynamically at runtime without local `.proto` files.
  """

  alias SimpleGrpc.Client.Conn
  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Reflection.{Descriptor, Server}
  alias SimpleGrpc.Schema

  @v1_path "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
  @v1_request "grpc.reflection.v1.ServerReflectionRequest"
  @v1_response "grpc.reflection.v1.ServerReflectionResponse"

  @v1alpha_path "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
  @v1alpha_request "grpc.reflection.v1alpha.ServerReflectionRequest"
  @v1alpha_response "grpc.reflection.v1alpha.ServerReflectionResponse"

  @doc """
  Queries the remote gRPC server for the list of registered services.

  ## Options
  - `:version` - `:auto` (default, tries v1 then v1alpha), `:v1`, or `:v1alpha`.
  - `:headers` - Additional headers/metadata to send with request.
  - `:timeout` - Timeout in milliseconds.

  ## Examples
      {:ok, services} = SimpleGrpc.Client.Reflection.list_services(conn)
      # => {:ok, ["streaming.StreamingService", "grpc.reflection.v1.ServerReflection"]}
  """
  def list_services(conn_or_target, opts \\ []) do
    with_conn(conn_or_target, opts, fn conn ->
      req = %{list_services: ""}

      case query_reflection(conn, req, opts) do
        {:ok, %{list_services_response: %{service: services}}} ->
          service_names =
            Enum.map(services, fn s ->
              Map.get(s, :name) || Map.get(s, "name")
            end)
            |> Enum.reject(&is_nil/1)

          {:ok, service_names}

        {:ok, other} ->
          {:error, 2, "Unexpected reflection response: #{inspect(other)}"}

        {:error, code, msg} ->
          {:error, code, msg}
      end
    end)
  end

  @doc """
  Queries the remote gRPC server for the file descriptor containing the given fully-qualified symbol name.

  Returns `{:ok, [binary_fdp_bytes, ...]}` containing the primary descriptor and its transitive dependencies.
  """
  def file_containing_symbol(conn_or_target, symbol, opts \\ []) when is_binary(symbol) do
    with_conn(conn_or_target, opts, fn conn ->
      req = %{file_containing_symbol: symbol}

      case query_reflection(conn, req, opts) do
        {:ok, %{file_descriptor_response: %{file_descriptor_proto: fdp_list}}} ->
          {:ok, fdp_list}

        {:ok, other} ->
          {:error, 2, "Unexpected reflection response: #{inspect(other)}"}

        {:error, code, msg} ->
          {:error, code, msg}
      end
    end)
  end

  @doc """
  Queries the remote gRPC server for the file descriptor by filename (e.g. `"streaming/streaming_service.proto"`).

  Returns `{:ok, [binary_fdp_bytes, ...]}` containing the primary descriptor and its transitive dependencies.
  """
  def file_by_filename(conn_or_target, filename, opts \\ []) when is_binary(filename) do
    with_conn(conn_or_target, opts, fn conn ->
      req = %{file_by_filename: filename}

      case query_reflection(conn, req, opts) do
        {:ok, %{file_descriptor_response: %{file_descriptor_proto: fdp_list}}} ->
          {:ok, fdp_list}

        {:ok, other} ->
          {:error, 2, "Unexpected reflection response: #{inspect(other)}"}

        {:error, code, msg} ->
          {:error, code, msg}
      end
    end)
  end

  @doc """
  Fetches and builds a complete `%SimpleGrpc.Schema{}` struct from the remote gRPC server.

  If a specific service or symbol name is given as the second argument, only that service
  and its dependencies are fetched. Otherwise, all registered services are discovered and fetched.

  ## Options
  - `:include_reflection` - (boolean, default: `false`) whether to include the reflection service itself in the schema.
  - `:only` - list of service names to include.
  - `:except` - list of service names to exclude.
  - `:services` - list of specific service names to include.
  - `:version` - `:auto`, `:v1`, or `:v1alpha`.
  """
  def fetch_schema(conn_or_target, opts_or_symbol \\ [])

  def fetch_schema(conn_or_target, symbol) when is_binary(symbol) do
    fetch_schema(conn_or_target, symbol, [])
  end

  def fetch_schema(conn_or_target, opts) when is_list(opts) do
    with_conn(conn_or_target, opts, fn conn ->
      with {:ok, all_services} <- list_services(conn, opts) do
        include_refl = Keyword.get(opts, :include_reflection, false)

        services_to_query =
          all_services
          |> Enum.reject(fn name ->
            not include_refl and String.starts_with?(name, "grpc.reflection.")
          end)
          |> maybe_filter_services(opts)

        schemas_result =
          Enum.reduce_while(services_to_query, {:ok, []}, fn svc_name, {:ok, acc_schemas} ->
            case file_containing_symbol(conn, svc_name, opts) do
              {:ok, fdp_bytes_list} ->
                decoded_schemas =
                  Enum.map(fdp_bytes_list, &Descriptor.decode_file_descriptor/1)

                {:cont, {:ok, acc_schemas ++ decoded_schemas}}

              {:error, code, msg} ->
                {:halt, {:error, code, "Reflection failed for #{svc_name}: #{msg}"}}
            end
          end)

        case schemas_result do
          {:ok, []} ->
            {:ok, %Schema{}}

          {:ok, schema_list} ->
            {:ok, Schema.merge(schema_list)}

          {:error, reason} ->
            {:error, reason}
        end
      end
    end)
  end

  def fetch_schema(conn_or_target, symbol, opts) when is_binary(symbol) and is_list(opts) do
    with_conn(conn_or_target, opts, fn conn ->
      case file_containing_symbol(conn, symbol, opts) do
        {:ok, fdp_bytes_list} ->
          decode_file_descriptors(fdp_bytes_list)

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  @doc """
  Decodes a list of binary `FileDescriptorProto` payloads and merges them into a single `%SimpleGrpc.Schema{}` struct.
  """
  def decode_file_descriptors(fdp_bytes_list) when is_list(fdp_bytes_list) do
    schemas = Enum.map(fdp_bytes_list, &Descriptor.decode_file_descriptor/1)
    {:ok, Schema.merge(schemas)}
  end

  # ---------------------------------------------------------------------------
  # Protocol Invocation & Version Negotiation
  # ---------------------------------------------------------------------------

  defp query_reflection(conn, req_map, opts) do
    version = Keyword.get(opts, :version, :auto)

    case version do
      :v1 ->
        invoke_version(conn, req_map, :v1, opts)

      :v1alpha ->
        invoke_version(conn, req_map, :v1alpha, opts)

      :auto ->
        case invoke_version(conn, req_map, :v1, opts) do
          {:ok, resp} ->
            {:ok, resp}

          {:error, 12, _} ->
            # Unimplemented in v1 -> fallback to v1alpha
            invoke_version(conn, req_map, :v1alpha, opts)

          {:error, 5, _} = err ->
            # Not found error from server (valid symbol lookup failure)
            err

          {:error, _code, _msg} = err ->
            # Try v1alpha as fallback on general error
            case invoke_version(conn, req_map, :v1alpha, opts) do
              {:ok, resp} -> {:ok, resp}
              _ -> err
            end
        end
    end
  end

  defp invoke_version(conn, req_map, :v1, opts) do
    schema = Server.reflection_v1_schema()
    req_bytes = Protobuf.encode!(@v1_request, req_map, schema)

    case execute_request(conn, @v1_path, req_bytes, opts) do
      {:ok, resp_bytes} ->
        decode_reflection_response(@v1_response, resp_bytes, schema)

      {:error, status, msg} ->
        {:error, status, msg}

      {:error, reason} ->
        {:error, 2, inspect(reason)}
    end
  end

  defp invoke_version(conn, req_map, :v1alpha, opts) do
    schema = Server.reflection_v1alpha_schema()
    req_bytes = Protobuf.encode!(@v1alpha_request, req_map, schema)

    case execute_request(conn, @v1alpha_path, req_bytes, opts) do
      {:ok, resp_bytes} ->
        decode_reflection_response(@v1alpha_response, resp_bytes, schema)

      {:error, status, msg} ->
        {:error, status, msg}

      {:error, reason} ->
        {:error, 2, inspect(reason)}
    end
  end

  defp execute_request(conn, path, req_bytes, opts) do
    case SimpleGrpc.Client.send_stream(conn, path, [req_bytes], opts) do
      {:ok, resp_stream, _headers, _trailers} ->
        chunk =
          cond do
            is_binary(resp_stream) ->
              resp_stream

            is_struct(resp_stream, Stream) or is_list(resp_stream) or is_function(resp_stream, 2) ->
              Enum.at(resp_stream, 0) || <<>>

            true ->
              <<>>
          end

        {:ok, chunk}

      {:error, 12, msg} ->
        {:error, 12, msg}

      {:error, _status, _msg} = err ->
        err

      {:error, _reason} ->
        case SimpleGrpc.Client.send_unary(conn, path, req_bytes, opts) do
          {:ok, resp_bytes, _headers, _trailers} -> {:ok, resp_bytes}
          err -> err
        end
    end
  end

  defp decode_reflection_response(resp_type, resp_bytes, schema) do
    case Protobuf.decode(resp_type, resp_bytes, schema) do
      {:ok, %{error_response: %{error_code: code, error_message: msg}}} when code != 0 ->
        {:error, code, msg}

      {:ok, resp_map} ->
        {:ok, resp_map}

      {:error, code, msg} ->
        {:error, code, "Failed to decode reflection response: #{msg}"}
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp with_conn(%Conn{} = conn, _opts, fun) when is_function(fun, 1) do
    fun.(conn)
  end

  defp with_conn(target, opts, fun) when is_function(fun, 1) do
    case SimpleGrpc.Client.connect(target, opts) do
      {:ok, conn} ->
        try do
          fun.(conn)
        after
          SimpleGrpc.Client.close(conn)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp maybe_filter_services(services, opts) do
    only = Keyword.get(opts, :only) || Keyword.get(opts, :services)
    except = Keyword.get(opts, :except)

    services
    |> maybe_filter_only(only)
    |> maybe_filter_except(except)
  end

  defp maybe_filter_only(services, nil), do: services

  defp maybe_filter_only(services, only) when is_list(only) do
    allowed = Enum.map(only, &to_string/1)
    Enum.filter(services, &(&1 in allowed))
  end

  defp maybe_filter_except(services, nil), do: services

  defp maybe_filter_except(services, except) when is_list(except) do
    disallowed = Enum.map(except, &to_string/1)
    Enum.reject(services, &(&1 in disallowed))
  end
end
