defmodule SimpleGrpc.Schema do
  @moduledoc """
  Represents a complete protobuf schema containing services, messages, and enums as runtime metadata.
  """

  alias SimpleGrpc.Schema.{Service, Message}
  alias SimpleGrpc.Schema.Enum, as: SchemaEnum

  defstruct [
    :package,
    syntax: :proto3,
    services: %{},
    messages: %{},
    enums: %{},
    options: %{}
  ]

  @type t :: %__MODULE__{
          package: String.t() | nil,
          syntax: atom(),
          services: %{String.t() => Service.t()},
          messages: %{String.t() => Message.t()},
          enums: %{String.t() => SchemaEnum.t()},
          options: map()
        }

  @doc """
  Builds a Schema struct from a `.proto` file path.
  """
  def from_file(file_path) do
    case SimpleGrpc.Parser.parse(file_path) do
      {:ok, syntax, _default_service, ast} ->
        {:ok, from_ast(ast, syntax: syntax)}

      {:error, reason} ->
        {:error, reason}
    end
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

    services =
      ast
      |> Enum.filter(fn
        {:service, _} -> true
        _ -> false
      end)
      |> Enum.map(fn s_ast ->
        service = Service.parse(s_ast, package)
        {service.name, service}
      end)
      |> Map.new()

    messages =
      ast
      |> Enum.filter(fn
        {:message, _} -> true
        _ -> false
      end)
      |> Enum.map(fn m_ast ->
        msg = Message.parse(m_ast)
        full_name = if package && package != "", do: "#{package}.#{msg.name}", else: msg.name
        msg = %{msg | full_name: full_name}
        {msg.name, msg}
      end)
      |> Map.new()

    enums =
      ast
      |> Enum.filter(fn
        {:enum, _} -> true
        _ -> false
      end)
      |> Enum.map(fn e_ast ->
        enum = SchemaEnum.parse(e_ast)
        {enum.name, enum}
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
      options: options
    }
  end

  @doc """
  Finds a service by name or full name.
  """
  def service(%__MODULE__{services: services, package: package}, name) do
    name_str = to_string(name)

    Map.get(services, name_str) ||
      (package && Map.get(services, String.replace_prefix(name_str, "#{package}.", ""))) ||
      Enum.find_value(services, fn {_k, s} -> if s.full_name == name_str, do: s end)
  end

  @doc """
  Finds a message by name or full name.
  """
  def message(%__MODULE__{messages: messages, package: package}, name) do
    name_str = to_string(name)

    Map.get(messages, name_str) ||
      (package && Map.get(messages, String.replace_prefix(name_str, "#{package}.", ""))) ||
      Enum.find_value(messages, fn {_k, m} -> if m.full_name == name_str, do: m end)
  end

  @doc """
  Finds an enum by name or full name across top-level enums and message nested enums.
  """
  def enum(%__MODULE__{enums: enums, messages: messages, package: package}, name) do
    name_str = name |> to_string() |> String.trim_leading(".")
    name_atom = if is_atom(name), do: name, else: String.to_atom(name_str)

    Map.get(enums, name) ||
      Map.get(enums, name_str) ||
      Map.get(enums, name_atom) ||
      (package && Map.get(enums, String.replace_prefix(name_str, "#{package}.", ""))) ||
      (package &&
         Map.get(
           enums,
           String.to_atom(String.replace_prefix(name_str, "#{package}.", ""))
         )) ||
      Enum.find_value(enums, fn {_k, e} ->
        if e.name == name_str or to_string(e.name) == name_str, do: e
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
end
