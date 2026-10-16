defmodule SimpleGrpc.Server do
  @moduledoc """
  Generates gRPC service implementation internals, in-module handler plumbing,
  and HTTP route matching for the using module based on a `.proto` file or `%SimpleGrpc.Schema{}` struct.

  ## Features
  - **Zero `protoc` dependency**: Parses `.proto` files at compile time in pure Elixir.
  - **Typed Message Structs**: Generates a struct for every message the service reaches (RPC types and the messages their fields use) under the server module, qualified by proto package (e.g. `MyApp.GreeterServer.Greeter.HelloRequest`).
  - **Flexible Handlers**: Supports in-module functions (with arity 1 or 2), external module/MFA references, and dynamic closures.
  - **Streaming Support**: Full HTTP/2 server streaming, client streaming, and bidirectional streaming.
  - **Seamless Plug Integration**: Plugs directly into `SimpleGrpc.Plug` on top of Bandit or Cowboy.

  ## Usage Example
  Define a service module with `use SimpleGrpc.Server`:

      defmodule MyApp.GreeterServer do
        use SimpleGrpc.Server,
          server_file: "priv/protos/greeter.proto"

        # Unary RPC handler: accepts (request, stream) or (request)
        def say_hello(req, _stream) do
          {:ok, %{message: "Hello, " <> req.name}}
        end

        # Server streaming RPC: returns a Stream or List of response maps/structs
        def lots_of_replies(req, _stream) do
          stream = Stream.map(1..req.count, fn i ->
            %{message: "Reply \#{i} to \#{req.name}"}
          end)
          {:ok, stream}
        end

        # Client streaming RPC: accepts a Stream of incoming requests
        def lots_of_greetings(req_stream, _stream) do
          names = Enum.map(req_stream, & &1.name)
          {:ok, %{summary: "Greetings to \#{Enum.join(names, ", ")}"}}
        end

        # Bidirectional streaming RPC: accepts req_stream and returns resp_stream
        def bidi_chat(req_stream, _stream) do
          resp_stream = Stream.map(req_stream, fn req ->
            %{response: "Echo: " <> req.message}
          end)
          {:ok, resp_stream}
        end
      end

  ## Server Options (`use SimpleGrpc.Server`)
  - `:files` - List of every `.proto` file the service needs, including the files it imports.
    Any expression that evaluates to a list of paths at compile time works. Imports are not
    searched for: each `import` must be a bundled well-known type or match the end of exactly
    one listed path, and a fully-qualified name may be defined by only one listed file.
    Violations are compile errors.

    Entries may be globs (`*`, `?`, `[...]`, `{a,b}`, `**`), e.g.
    `files: ["priv/protos/greeter/*.proto", "priv/protos/common/types.proto"]`. A glob is
    replaced by the regular `.proto` files it matches, sorted; it is a compile error if it
    matches none. The list keeps its order and each file appears once. Adding or removing a
    matching file recompiles the module. Prefer globs in the list over a `Path.wildcard/1`
    call for the whole list: only the former lets the module notice new files.
  - `:server_file` - Shorthand for `files: [path]` when one file is self-contained. It is a
    single path and is never globbed.
  - `:service` - Name of the proto `service` this module implements. Required when the files
    define more than one service; otherwise optional.
  - `:handlers` - (optional) Keyword list or map delegating specific RPCs to custom handler functions or modules.

  ## Handler Return Values
  Handlers can return:
  - `{:ok, response_map_or_struct}` - Successful response.
  - `{:ok, stream_or_enumerable}` - Successful streaming response.
  - `{:error, code, message}` - gRPC error with integer status code (e.g. `{:error, 5, "Not found"}`).
  - `{:error, message}` - gRPC error with default code `2` (`:unknown`).
  - Plain map or struct - Automatically treated as `{:ok, value}`.
  """

  alias SimpleGrpc.Server.Files
  alias SimpleGrpc.Server.Message
  alias SimpleGrpc.Server.Messages
  alias SimpleGrpc.Server.RPC

  defmacro __using__(opts) do
    caller_module = __CALLER__.module
    target_service = Keyword.get(opts, :service)

    {paths, glob_entries} = Files.resolve!(opts, __CALLER__)
    entries = Files.load!(paths, __CALLER__)

    {%Files.Entry{path: server_file, syntax: syntax, ast: ast}, service_name} =
      Files.pick_service!(entries, target_service, __CALLER__)

    if syntax != :proto3 do
      raise CompileError,
        file: __CALLER__.file,
        line: __CALLER__.line,
        description:
          "#{server_file} defines the service but is #{inspect(syntax)}; " <>
            "only proto3 service files are supported"
    end

    package =
      Enum.find_value(ast, fn
        {:package, pkg} when is_binary(pkg) -> pkg
        _ -> nil
      end)

    service_prefix =
      if package && package != "" do
        "#{package}.#{service_name}"
      else
        service_name
      end

    service_ast =
      Enum.find_value(ast, fn
        {:service, [^service_name | items]} -> items
        _ -> nil
      end)

    if is_nil(service_ast) do
      throw({:error, "service definition not found in #{server_file}"})
    end

    rpcs =
      service_ast
      |> Enum.filter(fn
        {:rpc, _} -> true
        _ -> false
      end)
      |> Enum.map(&RPC.parse/1)

    handlers_ast = Keyword.get(opts, :handlers, [])
    interceptors_ast = Keyword.get(opts, :interceptors, [])

    # Every message the service can reach gets a struct, namespaced by proto package.
    service_package = package || ""
    message_defs = Messages.collect!(entries, caller_module, rpcs, service_package, __CALLER__)
    message_index = Messages.index(message_defs)

    generated_modules_ast =
      Enum.map(message_defs, fn def ->
        Message.generate_module_ast(def.message, def.namespace, caller_module)
      end)

    messages_map = Messages.messages_map(message_defs, service_package)

    # `{rpc, input_module, output_module}`; a module is nil for a bundled well-known type
    rpc_modules =
      Enum.map(rpcs, fn rpc ->
        {rpc,
         Messages.type_module!(rpc.input, service_package, message_index, rpc.name, __CALLER__),
         Messages.type_module!(rpc.output, service_package, message_index, rpc.name, __CALLER__)}
      end)

    # Build canonical gRPC paths for each RPC
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

    # Build lookup maps for RPC inputs, outputs, and definitions (including paths)
    rpc_inputs =
      Enum.flat_map(rpc_modules, fn {rpc, mod, _output} ->
        path = "/#{service_prefix}/#{rpc.name}"
        [{path, mod} | expand_rpc_key_mappings(rpc.name, mod)]
      end)
      |> Map.new()

    rpc_outputs =
      Enum.flat_map(rpc_modules, fn {rpc, _input, mod} ->
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

    rpcs_escaped = Macro.escape(rpcs)
    messages_map_escaped = Macro.escape(messages_map)
    rpc_inputs_escaped = Macro.escape(rpc_inputs)
    rpc_outputs_escaped = Macro.escape(rpc_outputs)
    rpc_definitions_escaped = Macro.escape(rpc_definitions)
    path_to_rpc_escaped = Macro.escape(path_to_rpc)
    rpc_to_path_escaped = Macro.escape(rpc_to_path)

    schema_escaped = entries |> Files.merged_schema(paths) |> Macro.escape()

    external_resources_ast =
      Enum.map(paths, fn path ->
        quote do: @external_resource(unquote(path))
      end)

    # `@external_resource` only notices edits to files that already exist. With glob
    # entries, Mix also asks the module whether the expansion changed (a `.proto` was
    # added or removed) so it recompiles.
    mix_recompile_ast =
      if glob_entries do
        [
          quote do
            @__files_entries__ unquote(glob_entries)
            @__files_expanded__ unquote(paths)

            @doc false
            def __mix_recompile__?,
              do: SimpleGrpc.Server.Files.expand(@__files_entries__) != @__files_expanded__
          end
        ]
      else
        []
      end

    callbacks_ast =
      Enum.map(rpc_modules, fn {rpc, input_mod, output_mod} ->
        snake_name = String.to_atom(Macro.underscore(rpc.name))
        input_type = struct_type(input_mod)
        output_type = struct_type(output_mod)

        quote do
          @doc """
          Handler callback for RPC `#{unquote(rpc.name)}`.
          """
          @callback unquote(snake_name)(
                      request :: unquote(input_type) | map(),
                      context :: SimpleGrpc.Context.t() | term()
                    ) ::
                      {:ok, unquote(output_type) | map() | term()}
                      | {:error, integer(), String.t()}
                      | {:error, integer(), String.t(), term()}
                      | {:error, term()}
                      | unquote(output_type)
                      | map()

          @callback unquote(snake_name)(request :: unquote(input_type) | map()) ::
                      {:ok, unquote(output_type) | map() | term()}
                      | {:error, integer(), String.t()}
                      | {:error, integer(), String.t(), term()}
                      | {:error, term()}
                      | unquote(output_type)
                      | map()
        end
      end)

    optional_callbacks_list =
      Enum.flat_map(rpcs, fn rpc ->
        snake_name = String.to_atom(Macro.underscore(rpc.name))
        [{snake_name, 2}, {snake_name, 1}]
      end)

    quote do
      unquote_splicing(external_resources_ast)
      unquote_splicing(mix_recompile_ast)
      unquote_splicing(generated_modules_ast)

      @before_compile SimpleGrpc.Server

      unquote_splicing(callbacks_ast)
      @optional_callbacks unquote(optional_callbacks_list)

      @__schema__ unquote(schema_escaped)
      @__grpc_rpcs__ unquote(rpcs_escaped)
      @__grpc_opts_handlers__ unquote(handlers_ast)
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
      def __handlers__, do: unquote(handlers_ast)
      def __interceptors__, do: List.wrap(unquote(interceptors_ast))
      def __messages__, do: unquote(messages_map_escaped)

      @doc """
      Returns the canonical gRPC HTTP path for the given RPC name.
      """
      @spec grpc_path(rpc_name :: atom() | String.t()) :: String.t()
      def grpc_path(rpc_name) do
        case SimpleGrpc.Server.lookup_map(@__grpc_paths__, rpc_name) do
          nil ->
            raise ArgumentError, "Unknown RPC #{inspect(rpc_name)} for #{inspect(__MODULE__)}"

          path ->
            path
        end
      end

      @doc """
      Returns true if the HTTP path matches any RPC path of this service.
      """
      @spec matches_path?(path :: String.t()) :: boolean()
      def matches_path?(path) when is_binary(path) do
        SimpleGrpc.Server.has_key?(@__grpc_path_to_rpc__, path)
      end

      @doc """
      Returns routing information for the given HTTP path, or nil if unmatched.
      """
      @spec route_info(path :: String.t()) :: map() | nil
      def route_info(path) when is_binary(path) do
        case SimpleGrpc.Server.lookup_map(@__grpc_path_to_rpc__, path) do
          nil ->
            nil

          rpc_name ->
            rpc = SimpleGrpc.Server.lookup_map(@rpc_definitions, path)
            req_mod = SimpleGrpc.Server.lookup_map(@rpc_input_messages, path)
            resp_mod = SimpleGrpc.Server.lookup_map(@rpc_output_messages, path)

            %{
              rpc: rpc,
              rpc_name: rpc_name,
              path: path,
              service: __service_name__(),
              package: __grpc_package__(),
              input_module: req_mod,
              output_module: resp_mod
            }
        end
      end

      @doc """
      Builds a typed struct for the request of the given RPC or message name.
      """
      @spec build_request(rpc_or_msg :: atom() | String.t(), attrs :: map() | keyword()) ::
              struct()
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
      @spec build_response(rpc_or_msg :: atom() | String.t(), attrs :: map() | keyword()) ::
              struct()
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

      @doc """
      Dispatches an RPC call directly in-process to the server handlers.
      """
      @spec dispatch(
              rpc_or_path :: atom() | String.t(),
              request_payload :: term(),
              stream :: term()
            ) ::
              term()
      def dispatch(rpc_or_path, request_payload, stream \\ nil) do
        rpc =
          SimpleGrpc.Server.lookup_map(@rpc_definitions, rpc_or_path) ||
            raise ArgumentError,
                  "Unknown RPC call or path #{inspect(rpc_or_path)} in #{inspect(__MODULE__)}"

        req_mod =
          SimpleGrpc.Server.fetch_rpc_module!(
            @rpc_input_messages,
            rpc_or_path,
            "input",
            __MODULE__
          )

        resp_mod =
          SimpleGrpc.Server.fetch_rpc_module!(
            @rpc_output_messages,
            rpc_or_path,
            "output",
            __MODULE__
          )

        req =
          cond do
            rpc.request_stream and
                (is_struct(request_payload, Stream) or is_function(request_payload, 2)) ->
              Stream.map(request_payload, &SimpleGrpc.Server.build_message(req_mod, &1))

            is_struct(request_payload, req_mod) ->
              request_payload

            is_map(request_payload) or
                (is_list(request_payload) and Keyword.keyword?(request_payload)) ->
              SimpleGrpc.Server.build_message(req_mod, request_payload)

            rpc.request_stream and is_list(request_payload) ->
              Enum.map(request_payload, &SimpleGrpc.Server.build_message(req_mod, &1))

            true ->
              request_payload
          end

        case SimpleGrpc.Server.dispatch_handler(__MODULE__, __handlers__(), rpc.name, req, stream) do
          {:ok, data} ->
            cond do
              is_struct(data, Stream) or is_function(data, 2) ->
                Stream.map(data, &SimpleGrpc.Server.build_message(resp_mod, &1))

              is_list(data) and Keyword.keyword?(data) and data != [] ->
                SimpleGrpc.Server.build_message(resp_mod, data)

              is_list(data) and rpc.response_stream ->
                Enum.map(data, &SimpleGrpc.Server.build_message(resp_mod, &1))

              is_struct(data, resp_mod) ->
                data

              is_map(data) ->
                SimpleGrpc.Server.build_message(resp_mod, data)

              true ->
                data
            end

          {:error, _code, message} ->
            raise RuntimeError, message
        end
      end
    end
  end

  defmacro __before_compile__(env) do
    module = env.module
    rpcs = Module.get_attribute(module, :__grpc_rpcs__) || []
    opts_handlers = Module.get_attribute(module, :__grpc_opts_handlers__) || []
    opts_keys = extract_handler_keys(opts_handlers)

    missing_rpcs =
      Enum.filter(rpcs, fn rpc ->
        snake_atom = String.to_atom(Macro.underscore(rpc.name))
        orig_atom = String.to_atom(rpc.name)

        has_in_opts = has_handler?(opts_keys, rpc.name)

        has_in_module =
          Module.defines?(module, {snake_atom, 2}) or
            Module.defines?(module, {snake_atom, 1}) or
            Module.defines?(module, {orig_atom, 2}) or
            Module.defines?(module, {orig_atom, 1})

        not (has_in_opts or has_in_module)
      end)

    if missing_rpcs != [] do
      missing_names = Enum.map_join(missing_rpcs, ", ", &"\"#{&1.name}\"")
      example_func = Macro.underscore(hd(missing_rpcs).name)

      raise CompileError,
        description:
          "Missing handler for RPC call(s): #{missing_names} in #{inspect(module)}. " <>
            "Please define the in-module handler function (e.g. def #{example_func}(request, stream)) or provide a handler in the :handlers option.",
        file: env.file,
        line: env.line
    end

    nil
  end

  @doc """
  Looks up a key in a map, handling dynamic and empty map types cleanly.
  """
  def lookup_map(map, key) when is_map(map) do
    Map.get(map, key)
  end

  @doc """
  Checks if a key exists in a map, handling dynamic and empty map types cleanly.
  """
  def has_key?(map, key) when is_map(map) do
    Map.has_key?(map, key)
  end

  @doc """
  Fetches an RPC message module from the map. The module is `nil` for a bundled
  well-known type, which has no struct.
  """
  def fetch_rpc_module!(map, key, kind, caller_mod) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, mod} ->
        mod

      :error ->
        raise ArgumentError,
              "Could not find #{kind} message module for RPC #{inspect(key)} in #{inspect(caller_mod)}"
    end
  end

  @doc """
  Builds a message from `attrs` with `module`, or returns `attrs` unchanged when there
  is no struct module (`nil`, a bundled well-known type).
  """
  def build_message(nil, attrs), do: attrs
  def build_message(module, attrs), do: module.new(attrs)

  @doc """
  Looks up a message module by RPC name or direct message name.
  """
  def lookup_message_module(rpc_messages_map, message_modules_map, key) do
    Map.get(rpc_messages_map, key) || Map.get(message_modules_map, to_string(key))
  end

  @doc """
  Finds the handler for `rpc_name` in the given handler map or keyword list.
  """
  def find_handler_in(handlers, rpc_name) do
    snake_atom = String.to_atom(Macro.underscore(rpc_name))
    orig_atom = String.to_atom(rpc_name)
    snake_str = Macro.underscore(rpc_name)

    cond do
      Keyword.keyword?(handlers) and Keyword.has_key?(handlers, snake_atom) ->
        Keyword.get(handlers, snake_atom)

      Keyword.keyword?(handlers) and Keyword.has_key?(handlers, orig_atom) ->
        Keyword.get(handlers, orig_atom)

      is_map(handlers) and Map.has_key?(handlers, snake_atom) ->
        Map.get(handlers, snake_atom)

      is_map(handlers) and Map.has_key?(handlers, orig_atom) ->
        Map.get(handlers, orig_atom)

      is_map(handlers) and Map.has_key?(handlers, rpc_name) ->
        Map.get(handlers, rpc_name)

      is_map(handlers) and Map.has_key?(handlers, snake_str) ->
        Map.get(handlers, snake_str)

      is_list(handlers) ->
        Enum.find_value(handlers, fn
          {k, v} when k in [snake_atom, orig_atom, rpc_name, snake_str] -> v
          _ -> nil
        end)

      true ->
        nil
    end
  end

  @doc """
  Dispatches to either an in-module handler function or a configured opts handler,
  normalizing the return into `{:ok, result}` or `{:error, code, message}`.
  Exceptions are not rescued here — they propagate to the Plug layer for gRPC status mapping.
  """
  def dispatch_handler(caller_module, handlers, rpc_name, req, stream) do
    snake_atom = String.to_atom(Macro.underscore(rpc_name))
    orig_atom = String.to_atom(rpc_name)

    handler = find_handler_in(handlers, rpc_name)

    raw =
      cond do
        # 1. Configured handler in opts (closure, atom, MFA)
        not is_nil(handler) ->
          invoke_handler(handler, caller_module, req, stream)

        # 2. In-module snake_case function (arity 2)
        function_exported?(caller_module, snake_atom, 2) ->
          apply(caller_module, snake_atom, [req, stream])

        # 3. In-module snake_case function (arity 1)
        function_exported?(caller_module, snake_atom, 1) ->
          apply(caller_module, snake_atom, [req])

        # 4. In-module exact name function (arity 2)
        function_exported?(caller_module, orig_atom, 2) ->
          apply(caller_module, orig_atom, [req, stream])

        # 5. In-module exact name function (arity 1)
        function_exported?(caller_module, orig_atom, 1) ->
          apply(caller_module, orig_atom, [req])

        true ->
          {:error, 12,
           "No handler registered for RPC #{inspect(rpc_name)} in #{inspect(caller_module)}"}
      end

    normalize_handler_result(raw)
  end

  @doc """
  Invokes a handler function or MFA tuple with the given request and optional stream.
  Does not normalize — callers must call `normalize_handler_result/1`.
  """
  def invoke_handler(handler, caller_module, req, stream) do
    cond do
      is_function(handler, 2) ->
        handler.(req, stream)

      is_function(handler, 1) ->
        handler.(req)

      is_atom(handler) ->
        if function_exported?(caller_module, handler, 2) do
          apply(caller_module, handler, [req, stream])
        else
          apply(caller_module, handler, [req])
        end

      match?({mod, fun} when is_atom(mod) and is_atom(fun), handler) ->
        {mod, fun} = handler

        if function_exported?(mod, fun, 2) do
          apply(mod, fun, [req, stream])
        else
          apply(mod, fun, [req])
        end

      true ->
        {:error, 12, "Invalid handler #{inspect(handler)}"}
    end
  end

  @doc """
  Normalizes a raw handler return value into `{:ok, result}` or `{:error, code, message}`.
  Accepts plain maps/values, `{:ok, result}` tuples, and `{:error, ...}` tuples.
  """
  def normalize_handler_result({:ok, _} = ok), do: ok

  def normalize_handler_result({:error, code, msg}) when is_integer(code) and is_binary(msg),
    do: {:error, code, msg}

  def normalize_handler_result({:error, msg}) when is_binary(msg), do: {:error, 2, msg}
  def normalize_handler_result({:error, reason}), do: {:error, 2, inspect(reason)}
  def normalize_handler_result(result), do: {:ok, result}

  # Well-known types have no generated struct and stay plain maps.
  defp struct_type(nil), do: quote(do: map())
  defp struct_type(module), do: quote(do: unquote(module).t())

  defp expand_rpc_key_mappings(rpc_name, val) do
    snake_atom = String.to_atom(Macro.underscore(rpc_name))
    orig_atom = String.to_atom(rpc_name)
    snake_str = Macro.underscore(rpc_name)

    [
      {rpc_name, val},
      {snake_atom, val},
      {orig_atom, val},
      {snake_str, val}
    ]
  end

  defp extract_handler_keys(handlers_ast) do
    cond do
      Keyword.keyword?(handlers_ast) ->
        Keyword.keys(handlers_ast)

      is_list(handlers_ast) ->
        Enum.flat_map(handlers_ast, fn
          {k, _} -> [k]
          _ -> []
        end)

      match?({:%{}, _, _}, handlers_ast) ->
        {:%{}, _, kvs} = handlers_ast
        Enum.map(kvs, fn {k, _} -> k end)

      true ->
        []
    end
  end

  defp has_handler?(handler_keys, rpc_name) do
    snake_atom = String.to_atom(Macro.underscore(rpc_name))
    orig_atom = String.to_atom(rpc_name)
    snake_str = Macro.underscore(rpc_name)

    Enum.any?(handler_keys, fn key ->
      key in [snake_atom, orig_atom, rpc_name, snake_str]
    end)
  end
end
