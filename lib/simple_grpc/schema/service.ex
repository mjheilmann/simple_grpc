defmodule SimpleGrpc.Schema.Service do
  @moduledoc """
  Represents a parsed gRPC service descriptor.
  """

  alias SimpleGrpc.Schema.RPC

  defstruct [
    :name,
    :full_name,
    :package,
    :path_prefix,
    rpcs: %{},
    options: %{}
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          full_name: String.t(),
          package: String.t() | nil,
          path_prefix: String.t(),
          rpcs: %{String.t() => RPC.t()},
          options: map()
        }

  @doc """
  Parses a service AST node into a Service struct.
  """
  def parse({:service, [name | items]}, package \\ nil) when is_binary(name) do
    full_name =
      if package && package != "" do
        "#{package}.#{name}"
      else
        name
      end

    path_prefix = "/#{full_name}"

    rpcs =
      items
      |> Enum.filter(fn
        {:rpc, _} -> true
        _ -> false
      end)
      |> Enum.map(fn rpc_ast ->
        rpc = RPC.parse(rpc_ast)
        path = "#{path_prefix}/#{rpc.name}"
        %{rpc | path: path}
      end)
      |> Enum.map(fn %RPC{name: rpc_name} = rpc -> {rpc_name, rpc} end)
      |> Map.new()

    options =
      Enum.reduce(items, %{}, fn
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    %__MODULE__{
      name: name,
      full_name: full_name,
      package: package,
      path_prefix: path_prefix,
      rpcs: rpcs,
      options: options
    }
  end

  @doc """
  Finds an RPC by name or path.
  """
  def rpc(%__MODULE__{rpcs: rpcs, path_prefix: prefix}, name_or_path) do
    name_str = to_string(name_or_path)

    cond do
      Map.has_key?(rpcs, name_str) ->
        Map.get(rpcs, name_str)

      String.starts_with?(name_str, prefix <> "/") ->
        rpc_name = String.replace_prefix(name_str, prefix <> "/", "")
        Map.get(rpcs, rpc_name)

      true ->
        # Try matching by snake_case or case-insensitive if needed
        Enum.find_value(rpcs, fn {name, rpc} ->
          if Macro.underscore(name) == Macro.underscore(name_str) or name == name_str do
            rpc
          end
        end)
    end
  end
end
