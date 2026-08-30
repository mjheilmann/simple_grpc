defmodule SimpleGrpc.Server do
  @moduledoc """
  Generates gRPC service implementation internals for the using module based on a `.proto` file.
  """

  alias SimpleGrpc.Server.RPC
  alias SimpleGrpc.Server.Message

  defmacro __using__(opts) do
    caller_module = __CALLER__.module
    server_file = Keyword.fetch!(opts, :server_file)
    target_service = Keyword.get(opts, :service)

    {:proto3, default_service_name, ast} = SimpleGrpc.Parser.parse!(server_file)

    service_name = target_service || default_service_name

    service_ast =
      Enum.find_value(ast, fn
        {:service, [^service_name | items]} -> items
        _ -> nil
      end)

    if is_nil(service_ast) do
      raise ArgumentError, "Service #{inspect(service_name)} not found in #{server_file}"
    end

    rpcs =
      service_ast
      |> Enum.filter(fn
        {:rpc, _} -> true
        _ -> false
      end)
      |> Enum.map(&RPC.parse/1)

    handlers_ast = Keyword.get(opts, :handlers, [])
    handler_keys = extract_handler_keys(handlers_ast)

    missing_rpcs =
      Enum.filter(rpcs, fn rpc ->
        not has_handler?(handler_keys, rpc.name)
      end)

    if missing_rpcs != [] do
      missing_names = Enum.map_join(missing_rpcs, ", ", &"\"#{&1.name}\"")

      raise ArgumentError,
            "Missing handler for RPC call(s): #{missing_names} in #{inspect(caller_module)}. " <>
              "Please provide handlers in the :handlers option."
    end

    needed_message_names =
      rpcs
      |> Enum.flat_map(fn rpc ->
        [
          rpc.input |> String.split(".") |> List.first(),
          rpc.output |> String.split(".") |> List.first()
        ]
      end)
      |> Enum.uniq()

    message_asts =
      ast
      |> Enum.filter(fn
        {:message, [name | _]} -> name in needed_message_names
        _ -> false
      end)

    parsed_messages = Enum.map(message_asts, &Message.parse/1)

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

    # Build lookup maps for RPC inputs, outputs, and definitions
    rpc_inputs =
      Enum.flat_map(rpcs, fn rpc ->
        mod = Map.get(messages_map, rpc.input, Module.concat(caller_module, rpc.input))
        expand_rpc_key_mappings(rpc.name, mod)
      end)
      |> Map.new()

    rpc_outputs =
      Enum.flat_map(rpcs, fn rpc ->
        mod = Map.get(messages_map, rpc.output, Module.concat(caller_module, rpc.output))
        expand_rpc_key_mappings(rpc.name, mod)
      end)
      |> Map.new()

    rpc_definitions =
      Enum.flat_map(rpcs, fn rpc ->
        expand_rpc_key_mappings(rpc.name, rpc)
      end)
      |> Map.new()

    rpcs_escaped = Macro.escape(rpcs)
    messages_map_escaped = Macro.escape(messages_map)
    rpc_inputs_escaped = Macro.escape(rpc_inputs)
    rpc_outputs_escaped = Macro.escape(rpc_outputs)
    rpc_definitions_escaped = Macro.escape(rpc_definitions)

    quote do
      unquote_splicing(generated_modules_ast)

      @rpc_definitions unquote(rpc_definitions_escaped)
      @rpc_input_messages unquote(rpc_inputs_escaped)
      @rpc_output_messages unquote(rpc_outputs_escaped)
      @message_modules unquote(messages_map_escaped)

      def __service_name__, do: unquote(service_name)
      def __rpcs__, do: unquote(rpcs_escaped)
      def __handlers__, do: unquote(handlers_ast)
      def __messages__, do: unquote(messages_map_escaped)

      def build_request(rpc_or_msg, attrs \\ %{}) do
        case SimpleGrpc.Server.lookup_message_module(@rpc_input_messages, @message_modules, rpc_or_msg) do
          nil ->
            raise ArgumentError, "Unknown RPC or message #{inspect(rpc_or_msg)} in #{inspect(__MODULE__)}"

          message_mod ->
            message_mod.new(attrs)
        end
      end

      def build_response(rpc_or_msg, attrs \\ %{}) do
        case SimpleGrpc.Server.lookup_message_module(@rpc_output_messages, @message_modules, rpc_or_msg) do
          nil ->
            raise ArgumentError, "Unknown RPC or message #{inspect(rpc_or_msg)} in #{inspect(__MODULE__)}"

          message_mod ->
            message_mod.new(attrs)
        end
      end

      def dispatch(rpc_name, request_payload, stream \\ nil) do
        rpc =
          SimpleGrpc.Server.lookup_map(@rpc_definitions, rpc_name) ||
            raise ArgumentError, "Unknown RPC call #{inspect(rpc_name)} in #{inspect(__MODULE__)}"

        req_mod = SimpleGrpc.Server.fetch_rpc_module!(@rpc_input_messages, rpc_name, "input", __MODULE__)
        resp_mod = SimpleGrpc.Server.fetch_rpc_module!(@rpc_output_messages, rpc_name, "output", __MODULE__)

        req = req_mod.new(request_payload)

        handler =
          SimpleGrpc.Server.find_handler_in(__handlers__(), rpc.name) ||
            raise ArgumentError, "No handler registered for RPC #{inspect(rpc.name)} in #{inspect(__MODULE__)}"

        raw_resp = SimpleGrpc.Server.invoke_handler(handler, __MODULE__, req, stream)
        resp_mod.new(raw_resp)
      end
    end
  end

  @doc """
  Looks up a key in a map, handling dynamic and empty map types cleanly.
  """
  def lookup_map(map, key) when is_map(map) do
    Map.get(map, key)
  end

  @doc """
  Fetches an RPC message module from the map.
  """
  def fetch_rpc_module!(map, key, kind, caller_mod) when is_map(map) do
    case Map.get(map, key) do
      nil ->
        raise ArgumentError, "Could not find #{kind} message module for RPC #{inspect(key)} in #{inspect(caller_mod)}"

      mod ->
        mod
    end
  end

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
  Invokes a handler function or MFA tuple with the given request and optional stream.
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
        raise ArgumentError, "Invalid handler #{inspect(handler)}"
    end
  end

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
