defmodule SimpleGrpc.Client.DSL do
  @moduledoc """
  Generates strongly-typed client modules, RPC invocation functions, and message structs
  from a `.proto` definition or `%SimpleGrpc.Schema{}` struct.
  """

  alias SimpleGrpc.Server.RPC
  alias SimpleGrpc.Server.Message

  def generate(opts, caller_module) do
    file_or_schema =
      opts[:client_file] || opts[:server_file] || opts[:proto] || opts[:file] ||
        opts[:schema] ||
        raise ArgumentError,
              "Must specify :client_file or :schema option for use SimpleGrpc.Client"

    target_service = opts[:service]

    {ast, schema} =
      cond do
        is_binary(file_or_schema) ->
          {:proto3, parsed_ast} = SimpleGrpc.Parser.parse!(file_or_schema)
          s = SimpleGrpc.Schema.from_ast(parsed_ast)
          {parsed_ast, s}

        is_struct(file_or_schema, SimpleGrpc.Schema) ->
          # If already a schema struct, build ast if needed or extract services
          {nil, file_or_schema}

        true ->
          raise ArgumentError, "Invalid :client_file or :schema: #{inspect(file_or_schema)}"
      end

    default_service_name =
      if ast do
        Enum.find_value(ast, fn
          {:service, [name | _]} when is_binary(name) -> name
          _ -> nil
        end)
      else
        schema.services |> Map.keys() |> List.first()
      end

    service_name = target_service || default_service_name

    if is_nil(service_name) do
      raise ArgumentError, "No service definition found in #{inspect(file_or_schema)}"
    end

    package =
      if ast do
        Enum.find_value(ast, fn
          {:package, pkg} when is_binary(pkg) -> pkg
          _ -> nil
        end)
      else
        schema.package
      end

    service_prefix =
      if package && package != "" do
        "#{package}.#{service_name}"
      else
        service_name
      end

    rpcs =
      if ast do
        service_ast =
          Enum.find_value(ast, fn
            {:service, [^service_name | items]} -> items
            _ -> nil
          end)

        if is_nil(service_ast) do
          raise ArgumentError,
                "Service #{inspect(service_name)} not found in #{inspect(file_or_schema)}"
        end

        service_ast
        |> Enum.filter(fn
          {:rpc, _} -> true
          _ -> false
        end)
        |> Enum.map(&RPC.parse/1)
      else
        service_def =
          Map.get(schema.services, service_name) || Map.get(schema.services, service_prefix)

        if is_nil(service_def) do
          raise ArgumentError, "Service #{inspect(service_name)} not found in schema"
        end

        Enum.map(service_def.rpcs, fn {rpc_name, rpc} ->
          %RPC{
            name: rpc_name,
            input: rpc.input_message,
            output: rpc.output_message,
            request_stream: rpc.request_stream,
            response_stream: rpc.response_stream
          }
        end)
      end

    # Parse and generate message structs
    parsed_messages =
      if ast do
        needed_message_names =
          rpcs
          |> Enum.flat_map(fn rpc ->
            [
              rpc.input |> String.split(".") |> List.first(),
              rpc.output |> String.split(".") |> List.first()
            ]
          end)
          |> Enum.uniq()

        ast
        |> Enum.filter(fn
          {:message, [name | _]} -> name in needed_message_names
          _ -> false
        end)
        |> Enum.map(&Message.parse/1)
      else
        Enum.map(schema.messages, fn {_name, msg_def} -> msg_def end)
      end

    generated_modules_ast =
      Enum.map(parsed_messages, fn msg ->
        Message.generate_module_ast(msg, caller_module)
      end)

    messages_map =
      parsed_messages
      |> Enum.map(fn msg ->
        {msg.name, Module.concat(caller_module, msg.name)}
      end)
      |> Map.new()

    path_to_rpc =
      Enum.map(rpcs, fn rpc ->
        path = "/#{service_prefix}/#{rpc.name}"
        {path, rpc.name}
      end)
      |> Map.new()

    rpc_to_path =
      Enum.flat_map(rpcs, fn rpc ->
        path = "/#{service_prefix}/#{rpc.name}"
        expand_rpc_key_mappings(rpc.name, path)
      end)
      |> Map.new()

    rpc_inputs =
      Enum.flat_map(rpcs, fn rpc ->
        mod = Map.get(messages_map, rpc.input, Module.concat(caller_module, rpc.input))
        path = "/#{service_prefix}/#{rpc.name}"
        [{path, mod} | expand_rpc_key_mappings(rpc.name, mod)]
      end)
      |> Map.new()

    rpc_outputs =
      Enum.flat_map(rpcs, fn rpc ->
        mod = Map.get(messages_map, rpc.output, Module.concat(caller_module, rpc.output))
        path = "/#{service_prefix}/#{rpc.name}"
        [{path, mod} | expand_rpc_key_mappings(rpc.name, mod)]
      end)
      |> Map.new()

    rpc_definitions =
      Enum.flat_map(rpcs, fn rpc ->
        path = "/#{service_prefix}/#{rpc.name}"
        [{path, rpc} | expand_rpc_key_mappings(rpc.name, rpc)]
      end)
      |> Map.new()

    # Generate RPC invocation functions for each method
    rpc_functions_ast =
      Enum.map(rpcs, fn rpc ->
        generate_rpc_functions(rpc, service_prefix, caller_module, messages_map)
      end)

    rpcs_escaped = Macro.escape(rpcs)
    messages_map_escaped = Macro.escape(messages_map)
    rpc_inputs_escaped = Macro.escape(rpc_inputs)
    rpc_outputs_escaped = Macro.escape(rpc_outputs)
    rpc_definitions_escaped = Macro.escape(rpc_definitions)
    path_to_rpc_escaped = Macro.escape(path_to_rpc)
    rpc_to_path_escaped = Macro.escape(rpc_to_path)
    schema_escaped = Macro.escape(schema)
    interceptors_ast = Keyword.get(opts, :interceptors, [])

    quote do
      unquote_splicing(generated_modules_ast)

      @__schema__ unquote(schema_escaped)
      @__grpc_rpcs__ unquote(rpcs_escaped)
      @__grpc_package__ unquote(package)
      @__grpc_service_prefix__ unquote(service_prefix)
      @__grpc_service_path__ unquote("/" <> service_prefix)
      @__grpc_paths__ unquote(rpc_to_path_escaped)
      @__grpc_path_to_rpc__ unquote(path_to_rpc_escaped)
      @rpc_definitions unquote(rpc_definitions_escaped)
      @rpc_input_messages unquote(rpc_inputs_escaped)
      @rpc_output_messages unquote(rpc_outputs_escaped)
      @message_modules unquote(messages_map_escaped)

      def __schema__, do: @__schema__
      def __service_name__, do: unquote(service_name)
      def __grpc_package__, do: @__grpc_package__
      def __grpc_service_path__, do: @__grpc_service_path__
      def __grpc_paths__, do: @__grpc_paths__
      def __rpcs__, do: unquote(rpcs_escaped)
      def __interceptors__, do: List.wrap(unquote(interceptors_ast))
      def __messages__, do: unquote(messages_map_escaped)

      @doc """
      Returns the canonical gRPC HTTP path for the given RPC name.
      """
      def grpc_path(rpc_name) do
        case SimpleGrpc.Server.lookup_map(@__grpc_paths__, rpc_name) do
          nil ->
            raise ArgumentError, "Unknown RPC #{inspect(rpc_name)} for #{inspect(__MODULE__)}"

          path ->
            path
        end
      end

      @doc """
      Builds a typed struct for the request of the given RPC or message name.
      """
      def build_request(rpc_or_msg, attrs \\ %{}) do
        case SimpleGrpc.Server.lookup_message_module(
               @rpc_input_messages,
               @message_modules,
               rpc_or_msg
             ) do
          nil ->
            raise ArgumentError,
                  "Unknown RPC or message #{inspect(rpc_or_msg)} in #{inspect(__MODULE__)}"

          message_mod ->
            message_mod.new(attrs)
        end
      end

      @doc """
      Builds a typed struct for the response of the given RPC or message name.
      """
      def build_response(rpc_or_msg, attrs \\ %{}) do
        case SimpleGrpc.Server.lookup_message_module(
               @rpc_output_messages,
               @message_modules,
               rpc_or_msg
             ) do
          nil ->
            raise ArgumentError,
                  "Unknown RPC or message #{inspect(rpc_or_msg)} in #{inspect(__MODULE__)}"

          message_mod ->
            message_mod.new(attrs)
        end
      end

      unquote_splicing(rpc_functions_ast)
    end
  end

  defp generate_rpc_functions(rpc, service_prefix, caller_module, messages_map) do
    snake_name = String.to_atom(Macro.underscore(rpc.name))
    snake_bang = String.to_atom(Macro.underscore(rpc.name) <> "!")
    orig_name = String.to_atom(rpc.name)
    orig_bang = String.to_atom(rpc.name <> "!")

    path = "/#{service_prefix}/#{rpc.name}"
    input_mod = Map.get(messages_map, rpc.input, Module.concat(caller_module, rpc.input))
    output_mod = Map.get(messages_map, rpc.output, Module.concat(caller_module, rpc.output))
    input_msg_name = rpc.input
    output_msg_name = rpc.output

    if rpc.request_stream or rpc.response_stream do
      # Streaming RPC functions
      quote do
        @doc """
        Invokes the streaming RPC `#{unquote(rpc.name)}`.
        """
        def unquote(snake_name)(conn, request_arg, opts \\ []) do
          SimpleGrpc.Client.DSL.invoke_streaming_rpc(
            conn,
            unquote(path),
            unquote(input_mod),
            unquote(output_mod),
            unquote(input_msg_name),
            unquote(output_msg_name),
            @__schema__,
            request_arg,
            unquote(rpc.request_stream),
            unquote(rpc.response_stream),
            __interceptors__(),
            opts
          )
        end

        def unquote(orig_name)(conn, request_arg, opts \\ []) do
          unquote(snake_name)(conn, request_arg, opts)
        end

        def unquote(snake_bang)(conn, request_arg, opts \\ []) do
          case unquote(snake_name)(conn, request_arg, opts) do
            {:ok, result} -> result
            {:error, code, msg} -> raise RuntimeError, "gRPC error #{code}: #{msg}"
          end
        end

        def unquote(orig_bang)(conn, request_arg, opts \\ []) do
          unquote(snake_bang)(conn, request_arg, opts)
        end
      end
    else
      # Unary RPC functions
      quote do
        @doc """
        Invokes the unary RPC `#{unquote(rpc.name)}`.
        """
        def unquote(snake_name)(conn, request_arg, opts \\ []) do
          SimpleGrpc.Client.DSL.invoke_unary_rpc(
            conn,
            unquote(path),
            unquote(input_mod),
            unquote(output_mod),
            unquote(input_msg_name),
            unquote(output_msg_name),
            @__schema__,
            request_arg,
            __interceptors__(),
            opts
          )
        end

        def unquote(orig_name)(conn, request_arg, opts \\ []) do
          unquote(snake_name)(conn, request_arg, opts)
        end

        def unquote(snake_bang)(conn, request_arg, opts \\ []) do
          case unquote(snake_name)(conn, request_arg, opts) do
            {:ok, result} -> result
            {:error, code, msg} -> raise RuntimeError, "gRPC error #{code}: #{msg}"
          end
        end

        def unquote(orig_bang)(conn, request_arg, opts \\ []) do
          unquote(snake_bang)(conn, request_arg, opts)
        end
      end
    end
  end

  @doc false
  def invoke_unary_rpc(
        conn,
        path,
        input_mod,
        output_mod,
        input_msg_name,
        output_msg_name,
        schema,
        request_arg,
        opts
      )
      when is_list(opts) do
    invoke_unary_rpc(
      conn,
      path,
      input_mod,
      output_mod,
      input_msg_name,
      output_msg_name,
      schema,
      request_arg,
      [],
      opts
    )
  end

  def invoke_unary_rpc(
        conn,
        path,
        input_mod,
        output_mod,
        input_msg_name,
        output_msg_name,
        schema,
        request_arg,
        module_interceptors,
        opts
      ) do
    service_full_name = extract_service_prefix(path)
    service_name = extract_service_name(service_full_name)
    rpc_name = extract_rpc_name(path)
    merged_opts = SimpleGrpc.Client.Headers.merge_opts(conn, opts)
    headers = Keyword.get(merged_opts, :headers, [])

    conn_interceptors = (conn && conn.opts && Keyword.get(conn.opts, :interceptors)) || []
    call_interceptors = Keyword.get(opts, :interceptors, [])
    all_interceptors =
      List.wrap(conn_interceptors) ++ List.wrap(module_interceptors) ++ List.wrap(call_interceptors)

    ctx = %SimpleGrpc.Context{
      role: :client,
      service: service_name,
      service_name: service_full_name,
      rpc: rpc_name,
      rpc_name: rpc_name,
      path: path,
      request_stream: false,
      response_stream: false,
      headers: headers,
      metadata: Keyword.get(opts, :metadata, %{}),
      private: %{},
      opts: merged_opts,
      conn: conn,
      schema: schema
    }

    SimpleGrpc.Pipeline.execute(all_interceptors, request_arg, ctx, fn req, current_ctx ->
      call_opts = Keyword.put(current_ctx.opts, :headers, current_ctx.headers)

      meta = %{
        service: current_ctx.service,
        service_full_name: current_ctx.service_name,
        rpc: current_ctx.rpc_name,
        path: current_ctx.path,
        headers: current_ctx.headers,
        request: req,
        adapter: conn.adapter,
        conn: conn
      }

      SimpleGrpc.Client.Telemetry.span(meta, fn ->
        req_struct = coerce_request(req, input_mod)
        req_bytes = SimpleGrpc.Protobuf.encode!(input_msg_name, req_struct, schema)

        case SimpleGrpc.Client.send_unary(conn, path, req_bytes, call_opts) do
          {:ok, resp_bytes, _headers, _trailers} ->
            case SimpleGrpc.Protobuf.decode(output_msg_name, resp_bytes, schema) do
              {:ok, decoded_map} ->
                {:ok, output_mod.new(decoded_map)}

              {:error, _code, reason} ->
                {:error, 2, "Failed to decode response: #{reason}"}
            end

          {:error, status_code, status_msg} ->
            {:error, status_code, status_msg}

          {:error, reason} ->
            {:error, 2, inspect(reason)}
        end
      end)
    end)
  end

  @doc false
  def invoke_streaming_rpc(
        conn,
        path,
        input_mod,
        output_mod,
        input_msg_name,
        output_msg_name,
        schema,
        request_arg,
        request_stream?,
        response_stream?,
        opts
      )
      when is_list(opts) do
    invoke_streaming_rpc(
      conn,
      path,
      input_mod,
      output_mod,
      input_msg_name,
      output_msg_name,
      schema,
      request_arg,
      request_stream?,
      response_stream?,
      [],
      opts
    )
  end

  def invoke_streaming_rpc(
        conn,
        path,
        input_mod,
        output_mod,
        input_msg_name,
        output_msg_name,
        schema,
        request_arg,
        request_stream?,
        response_stream?,
        module_interceptors,
        opts
      ) do
    service_full_name = extract_service_prefix(path)
    service_name = extract_service_name(service_full_name)
    rpc_name = extract_rpc_name(path)
    merged_opts = SimpleGrpc.Client.Headers.merge_opts(conn, opts)
    headers = Keyword.get(merged_opts, :headers, [])

    conn_interceptors = (conn && conn.opts && Keyword.get(conn.opts, :interceptors)) || []
    call_interceptors = Keyword.get(opts, :interceptors, [])
    all_interceptors =
      List.wrap(conn_interceptors) ++ List.wrap(module_interceptors) ++ List.wrap(call_interceptors)

    ctx = %SimpleGrpc.Context{
      role: :client,
      service: service_name,
      service_name: service_full_name,
      rpc: rpc_name,
      rpc_name: rpc_name,
      path: path,
      request_stream: request_stream?,
      response_stream: response_stream?,
      headers: headers,
      metadata: Keyword.get(opts, :metadata, %{}),
      private: %{},
      opts: merged_opts,
      conn: conn,
      schema: schema
    }

    SimpleGrpc.Pipeline.execute(all_interceptors, request_arg, ctx, fn req, current_ctx ->
      call_opts = Keyword.put(current_ctx.opts, :headers, current_ctx.headers)

      meta = %{
        service: current_ctx.service,
        service_full_name: current_ctx.service_name,
        rpc: current_ctx.rpc_name,
        path: current_ctx.path,
        headers: current_ctx.headers,
        request: req,
        adapter: conn.adapter,
        conn: conn
      }

      SimpleGrpc.Client.Telemetry.span(meta, fn ->
        encoded_request_data =
          if request_stream? do
            cond do
              is_struct(req, Stream) or is_function(req, 2) or is_list(req) ->
                Stream.map(req, fn item ->
                  item_struct = coerce_request(item, input_mod)
                  SimpleGrpc.Protobuf.encode!(input_msg_name, item_struct, schema)
                end)

              true ->
                item_struct = coerce_request(req, input_mod)
                [SimpleGrpc.Protobuf.encode!(input_msg_name, item_struct, schema)]
            end
          else
            req_struct = coerce_request(req, input_mod)
            SimpleGrpc.Protobuf.encode!(input_msg_name, req_struct, schema)
          end

        case SimpleGrpc.Client.send_stream(conn, path, encoded_request_data, call_opts) do
          {:ok, resp_stream, _headers, _trailers} ->
            if response_stream? do
              decoded_stream =
                Stream.map(resp_stream, fn chunk ->
                  case SimpleGrpc.Protobuf.decode(output_msg_name, chunk, schema) do
                    {:ok, decoded_map} ->
                      output_mod.new(decoded_map)

                    {:error, _code, reason} ->
                      raise RuntimeError, "Failed to decode response chunk: #{reason}"
                  end
                end)

              {:ok, decoded_stream}
            else
              # Client streaming -> single response
              chunk =
                cond do
                  is_binary(resp_stream) ->
                    resp_stream

                  is_function(resp_stream, 2) or is_struct(resp_stream, Stream) or
                      is_list(resp_stream) ->
                    Enum.at(resp_stream, 0) || <<>>

                  true ->
                    <<>>
                end

              case SimpleGrpc.Protobuf.decode(output_msg_name, chunk, schema) do
                {:ok, decoded_map} ->
                  {:ok, output_mod.new(decoded_map)}

                {:error, _code, reason} ->
                  {:error, 2, "Failed to decode response: #{reason}"}
              end
            end

          {:error, status_code, status_msg} ->
            {:error, status_code, status_msg}

          {:error, reason} ->
            {:error, 2, inspect(reason)}
        end
      end)
    end)
  end

  defp extract_service_prefix(path) do
    trimmed = String.trim_leading(path, "/")

    case String.split(trimmed, "/") do
      [prefix, _rpc] -> prefix
      _ -> trimmed
    end
  end

  defp extract_service_name(service_prefix) do
    service_prefix
    |> String.split(".")
    |> List.last()
  end

  defp extract_rpc_name(path) do
    trimmed = String.trim_leading(path, "/")

    case String.split(trimmed, "/") do
      [_prefix, rpc] -> rpc
      _ -> trimmed
    end
  end

  defp coerce_request(payload, mod) do
    cond do
      is_struct(payload, mod) ->
        payload

      is_map(payload) or (is_list(payload) and Keyword.keyword?(payload)) ->
        mod.new(payload)

      true ->
        payload
    end
  end

  defp expand_rpc_key_mappings(rpc_name, value) do
    snake_str = Macro.underscore(rpc_name)
    snake_atom = String.to_atom(snake_str)
    orig_atom = String.to_atom(rpc_name)

    [
      {snake_atom, value},
      {orig_atom, value},
      {rpc_name, value},
      {snake_str, value}
    ]
  end
end
