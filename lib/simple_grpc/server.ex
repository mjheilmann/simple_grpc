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

    rpcs_escaped = Macro.escape(rpcs)
    messages_map_escaped = Macro.escape(messages_map)

    quote do
      unquote_splicing(generated_modules_ast)

      def __service_name__, do: unquote(service_name)
      def __rpcs__, do: unquote(rpcs_escaped)
      def __handlers__, do: unquote(handlers_ast)
      def __messages__, do: unquote(messages_map_escaped)
    end
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
